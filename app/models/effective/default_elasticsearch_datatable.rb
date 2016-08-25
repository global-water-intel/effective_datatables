module Effective
  class DefaultElasticsearchDatatable < Effective::Datatable
    attr_accessor :route_namespace
    attr_writer :select_overrides

    scopes do
      define_scopes.call
    end

    datatable do
      public_attributes_for_elasticsearch.each.with_index do |col, i|
        visibility = i < visible_column_limit
        if belongs_to_column?(col) || has_one_column?(col)
          if polymorphic_bt_column?(col)
            table_column col, visible: true, filter: { as: :text } do |record, _collection, datatable|
              assoc = record.send(col)

              next if assoc.blank?

              datatable.simple_link assoc
            end
          else
            table_column col, visible: visibility && belongs_to_visible?(col), filter: belongs_to_filter(col) do |record, _collection, datatable|
              assoc = record.send(col)

              next if assoc.blank?

              datatable.simple_link assoc
            end
          end
        elsif serialized_column?(col)
          table_column col, visible: visibility, type: type_for_attribute(col) do |record|
            record.send(col).to_s
          end
        elsif type_for_attribute(col) == :decimal
          table_column col, visible: visibility, type: type_for_attribute(col) do |record|
            number_with_delimiter record.send(col).to_f
          end
        elsif enumeration?(col)
          table_column col,
                       visible: visibility,
                       type: :enumeration,
                       filter: { type: :select, collection: enumeration_options(col) }
        elsif has_many?(col)
          table_column col, visible: visibility, type: type_for_attribute(col) do |record|
            record.send(col).map(&:to_s).join('<br><hr>')
          end
        else
          table_column col, visible: visibility, type: type_for_attribute(col)
        end
      end

      actions_column actions_column_options
      if save_state?
        bulk_actions_column do
          # bulk_action 'Test', test_path, data: { method: :post, resource_method: :id }
        end
      end
    end

    def actions_column_options
      { destroy: false }
    end

    def belongs_to_column?(col)
      record_class.reflections[col.to_s].try :belongs_to?
    end

    def enumeration?(col)
      record_class.defined_enums.keys.include?(col.to_s)
    end

    def enumeration_options(col)
      pluralized = col.pluralize
      record_class.send(pluralized).map { |k, _| [k, k] }
    end

    def polymorphic_bt_column?(col)
      belongs_to_column?(col) && record_class.reflections[col.to_s].try(:polymorphic?)
    end

    def has_many?(col)
      record_class.reflections[col.to_s].try :collection?
    end

    def has_one_column?(col)
      record_class.reflections[col.to_s].try :has_one?
    end

    def collection
      return @elasticsearch_collection if defined?(@elasticsearch_collection)

      base = record_class.es_query

      belongs_to_associations.each do |assoc|
        foreign_key = assoc.foreign_key.to_s
        a_id = attributes[foreign_key]

        next if a_id.blank?

        opts = { 'name' => foreign_key, 'type' => :foreign_key }
        base = base.dt_query a_id, opts
      end

      register_elasticsearch_aggregates(base)

      query_scopes(base)

      @elasticsearch_collection = base
    end

    def type_for_attribute(col)
      record_class.type_for_attribute(col.to_s).type
    end

    def serialized_column?(col)
      serialized_columns.include?(col.to_s)
    end

    def serialized_columns
      []
    end

    def aggregate_definitions
      {}
    end

    def aggregate_for(aggregate_name)
      field, type, options = aggregate_definitions[aggregate_name]

      searched_collection.aggregate_for(field, type, options)
    end

    def register_elasticsearch_aggregates(col)
      aggregate_definitions.each do |_, v|
        col.add_aggregate(*v)
      end
    end

    def query_scopes(col)
      col
    end

    def define_scopes
      -> {}
    end

    def search_column(collection, table_column, search_term, sql_column_or_array_index)
      name = table_column['name']

      if select_overrides.include?(name)
        tc = { 'name' => "#{name}_id", 'type' => :foreign_key }
        collection.dt_query(search_term, tc)
      else
        super
      end
    end

    # Override this function to perform custom ordering on a column
    # direction will be :asc or :desc
    def order_column(collection, table_column, direction, sql_column_or_array_index)
      super
    end

    def record_class
      self.class.name.demodulize.singularize.constantize
    end

    def public_attributes_for_elasticsearch
      record_class.public_attributes_for_elasticsearch
    end

    def visible_column_limit
      record_class.visible_column_limit
    end

    def belongs_to_associations
      record_class.reflections.values.select(&:belongs_to?)
    end

    def select_overrides
      @select_overrides ||= []
    end

    def belongs_to_filter(col)
      # return false if has_one_column?(col)

      assoc = record_class.reflections[col.to_s]

      return false if assoc.blank?

      klass = assoc.class_name.constantize

      return { as: :text } if klass.count > Effective::ElasticsearchQueryBuilder.select_size_limit

      select_overrides << col.to_s

      {
        as: :select,
        collection: klass.all.map { |e| [e.to_s, e.id] }.sort_by(&:first)
      }
    end

    def belongs_to_visible?(col)
      assoc = record_class.reflections[col.to_s]

      return false if assoc.blank?

      return true unless assoc.belongs_to? || assoc.has_one?

      attributes[assoc.foreign_key].blank?
    end

    def searched_collection
      elasticsearch_tool.search(collection)
    end

    def simple_link(record)
      link_label = record.to_s
      link_target = [route_namespace, record].select(&:present?)

      link_to link_label, link_target
    end
  end
end
