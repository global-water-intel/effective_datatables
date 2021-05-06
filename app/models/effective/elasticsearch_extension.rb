module Effective
  module ElasticsearchExtension
    extend ActiveSupport::Concern

    included do
      include Elasticsearch::Model
      include ElasticsearchExtras
    end

    module ClassMethods
      def iterate_results_instead_of_records
        false
      end

      def make_index_name(klass, main_name = nil)
        main_name ||= klass.table_name
        [
          Rails.application.class.parent.to_s.underscore,
          '_',
          main_name.underscore.pluralize,
          '_',
          Rails.env,
          Rails.configuration.elasticsearch_suffix
        ].join.downcase
      end

      def elasticsearch_initialize(index_name_override = nil)
        index_name make_index_name(self, index_name_override)
        document_type base_class.name.underscore
        settings_options = {
          index: {
            number_of_shards: 1,
            max_result_window: 9_999_999,
            requests: {
              cache: {
                enable: true
              }
            }
          },
          analysis: {
            tokenizer: {
              ngram_tokenizer: {
                type: :nGram,
                min_gram: 1,
                max_gram: 20,
                token_chars: %w(letter digit punctuation symbol)
              }
            },
            filter: {
              nGram_filter: {
                type: :nGram,
                min_gram: 1,
                max_gram: 20,
                token_chars: %w(letter digit punctuation symbol)
              },
              our_synonyms: {
                type: :synonym,
                synonyms: synonyms
              }
            },
            analyzer: {
              keyword_case_insensitive: {
                filter: %w(lowercase asciifolding),
                char_filter: %w(html_strip),
                type: :custom,
                tokenizer: :keyword
              },
              nGram_analyzer: {
                type: :custom,
                tokenizer: :whitespace,
                filter: %w(lowercase asciifolding nGram_filter our_synonyms),
                char_filter: %w(html_strip)
              },
              nGram_token_analyzer: {
                type: :custom,
                tokenizer: :ngram_tokenizer,
                filter: %w(lowercase asciifolding our_synonyms),
                char_filter: %w(html_strip)
              },
              whitespace_analyzer: {
                type: :custom,
                tokenizer: :whitespace,
                filter: %w(lowercase asciifolding),
                char_filter: %w(html_strip)
              },
              whitespace_case_insensitive: {
                type: :custom,
                tokenizer: :standard,
                filter: %w(lowercase asciifolding),
                char_filter: %w(html_strip)
              },
              whitespace_case_insensitive_synonyms: {
                filter: %w(lowercase asciifolding our_synonyms),
                char_filter: %w(html_strip),
                type: :custom,
                tokenizer: :standard
              },
              keyword_case_insensitive_synonyms: {
                filter: %w(lowercase asciifolding our_synonyms),
                char_filter: %w(html_strip),
                type: :custom,
                tokenizer: :keyword
              },
            }
          }
        }
        settings(settings_options) do
          mapping do
            klass = type.classify.constantize

            klass.columns.each do |column|
              name = column.name
              name_for_searching = klass.field_name_for_search(name)

              case column.type
              when :string
                indexes name, analyzer: :keyword_case_insensitive, fielddata: true
              when :text
                indexes name, analyzer: :keyword_case_insensitive, fielddata: true
              when :integer
                if klass.defined_enums.keys.include?(name)
                  indexes name, type: :keyword
                else
                  indexes name, type: :integer
                  indexes name_for_searching, type: :keyword

                  if klass.belongs_to_column?(column.name)
                    indexes name.gsub('_id', ''), analyzer: :keyword_case_insensitive, fielddata: true
                    indexes "#{name.to_s.gsub('_id', '')}_raw", type: :keyword
                  end
                end
              when :datetime, :date
                indexes name, type: :date, format: :date
                indexes name_for_searching, type: :keyword
              when :decimal, :float
                indexes name, type: :float
                indexes name_for_searching, type: :keyword
              when :boolean
                indexes name, type: :boolean
              when :spatial
                # TODO: figure out what we do with spatial columns. Ignoring at the moment.
              when :uuid
                indexes name, type: :keyword
              when nil
                # TODO: hack to deal with spatial columns being borked in Rails 5
              else
                raise NotImplementedError, "Don't know how to handle ActiveRecord column type: `#{type}`."
              end
            end

            klass.reflections.values.each do |reflection|

              if reflection.has_one?
                name = reflection.name

                indexes name, analyzer: :keyword_case_insensitive, fielddata: true
                indexes "#{name}_id", type: :integer
                indexes "#{name.to_s.gsub('_id', '')}_raw", type: :keyword
              end
            end

            klass.index_lambdas.each { |lmbda| instance_exec('', &lmbda) }
          end
        end

        def inherited(subclass)
          super

          subclass.elasticsearch_initialize
        end
      end

      def register_es_index_lambda_before_initialize(&block)
        index_lambda_list << block
      end

      def index_lambda_list
        @index_lambda_list ||= []
      end

      def index_lambdas
        index_lambda_list + [indexes_extras]
      end

      def indexes_extras
        return ->(_) {} unless respond_to?(:elasticsearch_index_hook)

        elasticsearch_index_hook
      end

      def visible_column_limit
        7
      end

      def public_attributes_for_elasticsearch
        public_attributes
      end

      def es_query
        ElasticsearchQueryBuilder
          .new(self)
          .tap { |e| e.iterate_results_instead_of_records = iterate_results_instead_of_records }
      end

      def belongs_to_column?(column)
        id_removed = column.gsub('_id', '')

        reflections[id_removed].try(:belongs_to?)
      end

      def field_name_for_search(field)
        "#{field}_for_searching"
      end

      def debug_es_import
        __elasticsearch__.import force: true, scope: :for_elasticsearch, return: 'errors'
      end

      def es_import
        __elasticsearch__.import force: true, scope: :for_elasticsearch
      end

      def es_update_mapping
        Elasticsearch::Model.client.indices.put_mapping index: index_name, type: base_class.name.underscore, body: mapping.to_hash
      end
    end

    def as_indexed_json_hook(default_json)
      default_json
    end

    def as_indexed_json_base
      h = {}
      klass = self.class

      klass.columns.each do |column|
        name = column.name
        name_for_searching = self.class.field_name_for_search(name)

        case column.type
        when :string
          h[name] = send name
        when :text
          check = send name

          h[name] = check.is_a?(String) ? check : check.to_s
        when :integer
          if klass.defined_enums.keys.include?(name)
            h[name] = send(name)
          else
            h[name] = send name
            h[name_for_searching] = h[name].to_s

            if klass.belongs_to_column?(column.name)
              assoc = column.name.gsub('_id', '')
              h[assoc] = h["#{assoc}_raw"] = send(assoc).try(:to_s)
            end
          end
        when :datetime, :date
          # TODO: be sure this is formatted such that we can search/sort with it
          h[name] = send(name).try(:to_date).try :to_formatted_s, :db
          h[name_for_searching] = h[name].to_s
        when :decimal, :float
          h[name] = send(name).try(:to_f)
          h[name_for_searching] = h[name].to_s
        when :boolean
          h[name] = send(name)
        when :spatial
          # TODO: figure out what we do with spatial columns. Ignoring at the moment.
        when :uuid
          h[name] = send(name).to_s
        else
          raise NotImplementedError, "Don't know how to handle ActiveRecord column type: `#{type}`."
        end
      end

      klass.reflections.values.each do |reflection|
        next unless reflection.has_one?

        name = reflection.name

        h[name] = h["#{name}_raw"] = send(name).try(:to_s)
        h["#{name}_id"] = send(name).try(:id)
      end

      h
    end

    def as_indexed_json(_options = {})
      h = as_indexed_json_base

      as_indexed_json_hook(h)

      h
    end
  end
end
