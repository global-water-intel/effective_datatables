module Effective
  class ActiveRecordDatatableTool
    attr_accessor :table_columns

    delegate :order_column_index, :order_direction, :page, :per_page, :search_column, :to => :@datatable

    def initialize(datatable, table_columns)
      @datatable = datatable
      @table_columns = table_columns
    end

    def order_column
      @order_column ||= table_columns.find { |_, values| values[:index] == order_column_index }.try(:second) # This pulls out the values
    end

    def search_terms
      @search_terms ||= @datatable.search_terms.select { |name, search_term| table_columns.key?(name) }
    end

    def order(collection)
      return collection if order_column.blank?

      # if [:string, :text].include?(order_column[:type])
      #   collection.order("COALESCE(#{order_column[:column]}, '') #{order_direction}")
      # else
      #   collection.order("#{order_column[:column]} #{order_direction}")
      # end

      collection.order("#{order_column[:column]} #{order_direction}")
    end

    def search(collection)
      search_terms.each do |name, search_term|
        column_search = search_column(collection, table_columns[name], search_term)
        raise 'search_column must return an ActiveRecord::Relation object' unless column_search.kind_of?(ActiveRecord::Relation)
        collection = column_search
      end
      collection
    end

    def search_column_with_defaults(collection, table_column, term)
      column = table_column[:column]

      case table_column[:type]
      when :string, :text
        if table_column[:filter][:type] == :select && table_column[:filter][:fuzzy] != true
          collection.where("LOWER(#{column}) = LOWER(:term)", :term => term)
        else
          build_conditions_for(term, column, collection)
        end
      when :datetime
        begin
          digits = term.scan(/(\d+)/).flatten.map(&:to_i)
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

          collection.where("#{column} >= :start_at AND #{column} <= :end_at", :start_at => start_at, :end_at => end_at)
        rescue => e
          collection
        end
      when :obfuscated_id
        if (deobfuscated_id = collection.deobfuscate(term)) == term # We weren't able to deobfuscate it, so this is an Invalid ID
          collection.where("#{column} = :term", :term => 0)
        else
          collection.where("#{column} = :term", :term => deobfuscated_id)
        end
      when :integer
        collection.where("#{column} = :term", :term => term.to_i)
      when :year
        collection.where("EXTRACT(YEAR FROM #{column}) = :term", :term => term.to_i)
      when :boolean
        if term == 'true'
          collection.where("#{column} = ?", true)
        elsif term == 'false'
          collection.where("#{column} = ?", false)
        else
          collection
        end
      else
        collection.where("#{column} = :term", :term => term)
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

  end
end
