module Effective
  class ElasticsearchDatatableTool
    attr_accessor :table_columns

    delegate :page, :per_page, :search_column, :order_column, :collection_class, :quote_sql, :to => :@datatable

    def initialize(datatable, table_columns)
      @datatable = datatable
      @table_columns = table_columns
    end

    def search_terms
      @search_terms ||= @datatable.search_terms.select { |name, search_term| table_columns.key?(name) }
    end

    def order_by_column
      @order_by_column ||= table_columns[@datatable.order_name]
    end

    def order(collection)
      return collection unless order_by_column.present?

      column_order = order_column(collection, order_by_column, @datatable.order_direction, order_by_column[:column])
      raise 'order_column must return an EsQueryBuilder object' unless column_order.kind_of?(EsQueryBuilder)
      column_order
    end

    def order_column_with_defaults(collection, table_column, direction, sql_column)
      before = ''; after = ''
      sql_direction = (direction == :desc ? 'DESC' : 'ASC')

      if table_column[:type] == :belongs_to_polymorphic
        collection.order("#{before}#{sql_column.sub('_id', '_type')} #{sql_direction}, #{sql_column} #{sql_direction}#{after}")
      elsif table_column[:sql_as_column] == true
        collection.order("#{sql_column} #{sql_direction}")
      else
        collection.order("#{before}#{sql_column} #{sql_direction}#{after}")
      end
    end

    def search(collection)
      search_terms.each do |name, search_term|
        column_search = search_column(collection, table_columns[name], search_term, table_columns[name][:column])
        raise 'search_column must return an EsQueryBuilder object' unless column_search.kind_of?(EsQueryBuilder)
        collection = column_search
      end
      collection
    end

    def search_column_with_defaults(collection, table_column, term, sql_column)
      collection.dt_query(table_column, term)
    end

    def paginate(collection)
      collection.page(page).per(per_page)
    end

    def total_entries(collection = nil)
      @total_entries ||= collection.total_entries
    end

    def unfiltered_total_entries(collection)
      @unfiltered_total_entries ||= collection.unfiltered_total_entries
    end
  end
end
