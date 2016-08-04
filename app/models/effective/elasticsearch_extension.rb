module Effective
  module ElasticsearchExtension
    extend ActiveSupport::Concern

    included do
      include Elasticsearch::Model
      index_name "#{table_name}_#{Rails.env}".downcase
      settings_options = {
        index: {
          number_of_shards: 1,
          cache: {
            query: {
              enable: true
            }
          }
        },
        analysis: {
          analyzer: {
            case_insensitive: {
              filter: ['lowercase'],
              type: 'custom',
              tokenizer: 'keyword'
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
            when :string, :text
              indexes name, analyzer: :case_insensitive
            when :integer
              if klass.defined_enums.keys.include?(name)
                indexes name, index: :not_analyzed, type: :string
              else
                indexes name, index: :not_analyzed, type: :integer
                indexes name_for_searching, index: :not_analyzed, type: :string

                indexes name.gsub('_id', ''), analyzer: :case_insensitive if klass.belongs_to_column?(column.name)
              end
            when :datetime
              indexes name, index: :not_analyzed, type: :date, format: :date
              indexes name_for_searching, index: :not_analyzed, type: :string
            when :decimal
              indexes name, index: :not_analyzed, type: :float
              indexes name_for_searching, index: :not_analyzed, type: :string
            when :boolean
              indexes name, index: :not_analyzed, type: :boolean
            when :spatial
              # TODO: figure out what we do with spatial columns. Ignoring at the moment.
            when :uuid
              indexes name, index: :not_analyzed, type: :string
            else
              binding.pry
            end
          end

          klass.reflections.values.each do |reflection|

            if reflection.has_one?
              name = reflection.name

              indexes name, analyzer: :case_insensitive
              indexes "#{name}_id", index: :not_analyzed, type: :integer
            end
          end
        end
      end
    end

    module ClassMethods
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

    def as_indexed_json(_options = {})
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
              h[assoc] = send(assoc).try(:to_s)
            end
          end
        when :datetime
          # TODO: be sure this is formatted such that we can search/sort with it
          h[name] = send(name).try(:to_date).try :to_formatted_s, :db
          h[name_for_searching] = h[name].to_s
        when :decimal
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

        if reflection.has_one?
          name = reflection.name

          h[name] = send(name).try(:to_s)
          h["#{name}_id"] = send(name).try(:id)
        end
      end

      h
    end
  end
end
