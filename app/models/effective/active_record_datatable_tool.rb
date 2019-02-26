module Effective
  class ActiveRecordDatatableTool
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
      raise 'order_column must return an ActiveRecord::Relation object' unless column_order.kind_of?(ActiveRecord::Relation)
      column_order
    end

    def order_column_with_defaults(collection, table_column, direction, sql_column)
      before = ''; after = ''
      sql_direction = (direction == :desc ? 'DESC' : 'ASC')

      if postgres?
        after = if table_column[:nulls] == :first
          ' NULLS FIRST'
        elsif table_column[:nulls] == :last
          ' NULLS LAST'
        else
          " NULLS #{direction == :desc ? 'FIRST' : 'LAST' }"
        end
      elsif mysql?
        before = "ISNULL(#{sql_column}), "
      end

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
        raise 'search_column must return an ActiveRecord::Relation object' unless column_search.kind_of?(ActiveRecord::Relation)
        collection = column_search
      end
      collection
    end

    def search_column_with_defaults(collection, table_column, term, sql_column)
      sql_op = table_column[:filter][:sql_operation] || :where # only other option is :having

      case table_column[:type]
      when :string, :text
        if table_column[:filter][:type] == :select && table_column[:filter][:fuzzy] != true
          collection.where("LOWER(#{column}) = LOWER(:term)", :term => term)
        else
          build_conditions_for(term, column, collection)
        end

        collection.public_send(sql_op, id: ids)
      when :obfuscated_id
        if (deobfuscated_id = collection.deobfuscate(term)) == term # We weren't able to deobfuscate it, so this is an Invalid ID
          collection.public_send(sql_op, "#{sql_column} = :term", term: 0)
        else
          collection.public_send(sql_op, "#{sql_column} = :term", term: deobfuscated_id)
        end
      when :effective_address
        ids = Effective::Address
          .where('addressable_type = ?', collection_class.name)
          .where("address1 #{ilike} :term OR address2 #{ilike} :term OR city #{ilike} :term OR postal_code #{ilike} :term OR state_code = :code OR country_code = :code", term: "%#{term}%", code: term)
          .pluck(:addressable_id)

        collection.public_send(sql_op, id: ids)
      when :effective_roles
        collection.with_role(term)
      when :datetime, :date
        begin
          digits = term.scan(/(\d+)/).flatten.map { |digit| digit.to_i }
          start_at = Time.zone.local(*digits)

          case digits.length
          when 1  # Year
            end_at = start_at.end_of_year
          when 2 # Year-Month
            end_at = start_at.end_of_month
          when 3 # Year-Month-Day
            end_at = start_at.end_of_day
          when 4 # Year-Month-Day Hour
            end_at = start_at.end_of_hour
          when 5 # Year-Month-Day Hour-Minute
            end_at = start_at.end_of_minute
          when 6
            end_at = start_at + 1.second
          else
            end_at = start_at
          end

          collection.public_send(sql_op, "#{sql_column} >= :start_at AND #{sql_column} <= :end_at", start_at: start_at, end_at: end_at)
        rescue => e
          collection
        end
      when :boolean
        collection.public_send(sql_op, "#{sql_column} = :term", term: [1, 'true', 'yes'].include?(term.to_s.downcase))
      when :integer
        collection.public_send(sql_op, "#{sql_column} = :term", term: term.gsub(/\D/, '').to_i)
      when :year
        collection.public_send(sql_op, "EXTRACT(YEAR FROM #{sql_column}) = :term", term: term.to_i)
      when :price
        price_in_cents = (term.gsub(/[^0-9|\.]/, '').to_f * 100.0).to_i
        collection.public_send(sql_op, "#{sql_column} = :term", term: price_in_cents)
      when :currency, :decimal, :number, :percentage
        collection.public_send(sql_op, "#{sql_column} = :term", term: term.gsub(/[^0-9|\.]/, '').to_f)
      else
        collection.public_send(sql_op, "#{sql_column} = :term", term: term)
      end
    end

    def paginate(collection)
      collection.per_page_kaminari(page).per(per_page)
    end

    def build_conditions_for(terms, column, collection)
      if terms.match(/\".*\"/)
        splits = [terms.gsub('"', "%")]
      else
        splits = terms.split.map { |w| "%#{w}%" }
      end

      splits.reduce(collection) do |col, word|
        col.where("LOWER(#{column}) LIKE LOWER(:term)", term: word)
      end
    end

    protected

    def postgres?
      return @postgres unless @postgres.nil?
      @postgres ||= (collection_class.connection.kind_of?(ActiveRecord::ConnectionAdapters::PostgreSQLAdapter) rescue false)
    end

    def mysql?
      return @mysql unless @mysql.nil?
      @mysql ||= (collection_class.connection.kind_of?(ActiveRecord::ConnectionAdapters::Mysql2Adapter) rescue false)
    end

    def ilike
      @ilike ||= (postgres? ? 'ILIKE' : 'LIKE')  # Only Postgres supports ILIKE, Mysql and Sqlite3 use LIKE
    end

  end
end
