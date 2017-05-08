module Effective
  module ElasticsearchExtension
    extend ActiveSupport::Concern

    included do
      include Elasticsearch::Model
      include ElasticsearchExtras
    end

    module ClassMethods
      def elasticsearch_initialize
        index_name "#{Rails.application.class.parent.to_s.underscore}_#{table_name}_#{Rails.env}#{Rails.configuration.elasticsearch_suffix}".downcase
        settings_options = {
          index: {
            number_of_shards: 1,
            max_result_window: 9_999_999
            # cache: {
            #   query: {
            #     enable: false
            #   }
            # }
          },
          analysis: {
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
              case_insensitive: {
                filter: %w(lowercase asciifolding),
                type: 'custom',
                tokenizer: 'keyword'
              },
              nGram_analyzer: {
                type: :custom,
                tokenizer: :whitespace,
                filter: %w(lowercase asciifolding nGram_filter our_synonyms)
              },
              whitespace_analyzer: {
                type: :custom,
                tokenizer: :whitespace,
                filter: %w(lowercase asciifolding)
              }
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
                indexes name, analyzer: :case_insensitive
              when :text
                indexes name, analyzer: :case_insensitive
              when :integer
                if klass.defined_enums.keys.include?(name)
                  indexes name, index: :not_analyzed, type: :string
                else
                  indexes name, index: :not_analyzed, type: :integer
                  indexes name_for_searching, index: :not_analyzed, type: :string

                  if klass.belongs_to_column?(column.name)
                    indexes name.gsub('_id', ''), analyzer: :case_insensitive
                    indexes "#{name.to_s.gsub('_id', '')}_raw", index: :not_analyzed
                  end
                end
              when :datetime, :date
                indexes name, index: :not_analyzed, type: :date, format: :date
                indexes name_for_searching, index: :not_analyzed, type: :string
              when :decimal, :float
                indexes name, index: :not_analyzed, type: :float
                indexes name_for_searching, index: :not_analyzed, type: :string
              when :boolean
                indexes name, index: :not_analyzed, type: :boolean
              when :spatial
                # TODO: figure out what we do with spatial columns. Ignoring at the moment.
              when :uuid
                indexes name, index: :not_analyzed, type: :string
              when nil
                # TODO: hack to deal with spatial columns being borked in Rails 5
              else
                binding.pry
              end
            end

            klass.reflections.values.each do |reflection|

              if reflection.has_one?
                name = reflection.name

                indexes name, analyzer: :case_insensitive
                indexes "#{name}_id", index: :not_analyzed, type: :integer
                indexes "#{name.to_s.gsub('_id', '')}_raw", index: :not_analyzed
              end
            end

            klass.index_lambdas.each { |lmbda| instance_exec('', &lmbda) }
          end
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
        ElasticsearchQueryBuilder.new(self)
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
          binding.pry
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
