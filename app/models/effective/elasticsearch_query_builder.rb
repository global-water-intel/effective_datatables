module Effective
  class ElasticsearchQueryBuilder
    include Enumerable

    attr_accessor :active_record_klass, :search_options, :page_number, :page_size

    def self.select_size_limit
      50
    end

    def initialize(klass)
      self.active_record_klass = klass
      self.search_options = {
        query: {
          filtered: {
            filter: {
              bool: {
                must: [
                ]
              }
            }
          }
        },
        sort: [],
        aggs: {}
      }

      self.page_size = 25
    end

    def each(&block)
      records.each(&block)
    end

    def records
      response.records.for_elasticsearch
    end

    def dt_query(table_column, term)
      name = table_column['name']
      name_for_searching = active_record_klass.field_name_for_search(name)

      case table_column['type']
      when :string, :text, :enumeration
        query wildcard: { name => "*#{term.downcase}*" }
        # query match: { name => term }
      when :foreign_key, :exact_string
        filter term: { name => term }
      when :datetime
        query wildcard: { name_for_searching => "*#{term}*" }
      else
        query wildcard: { name_for_searching => "*#{term.to_s.downcase}*" }
        # query match: { name => term }
      end
    end

    def filter(hash = {})
      search_options[:query][:filtered][:filter][:bool][:must] << hash

      self
    end

    def query(hash = {})
      search_options[:query][:filtered][:filter][:bool][:must] << { query: hash }

      self
    end

    def order(h = {})
      hash = h.is_a?(Hash) ? h : Hash[[h.downcase.split(' ')]]
      search_options[:sort] << hash
      self
    end

    def page(page_number = 1)
      self.page_number = page_number

      self
    end

    def per(page_size)
      self.page_size = page_size

      self
    end

    def total_entries
      response.total_entries
    end

    def unfiltered_total_entries
      @unfiltered_total_entries ||= execute_unfiltered_count!
    end

    def aggregate_for(type, field, options = {})
      es_agg = ElasticsearchAggregate.new(type, field, options)

      es_agg.results_as_hash_from(aggregations)
    end

    def add_aggregate(type, field, options = {})
      es_agg = ElasticsearchAggregate.new(type, field, options)

      search_options[:aggs] = es_agg.register_options(search_options[:aggs])
    end

    def to_array_and_total_entries
      @cache_execute = true
      @arrayified ||= to_a
      @total_entries ||= total_entries

      [@arrayified, @total_entries]
    end

    private

    def aggregations
      @agg_cache ||= response.aggregations
    end

    def response
      execute!
    end

    def execute!
      return @execute_result if @cache_execute && defined?(@execute_result)

      # Don't modify #search_options once we've performed the search.
      # The search won't get run again.
      # Any modifications would just have no effect.
      # Not sure if there's a point to juggling @cache_execute if we're just gonna freeze always.
      search_options.freeze

      ex = active_record_klass.__elasticsearch__.search(search_options).page(page_number).per_page(page_size)

      return @execute_result = ex if @cache_execute
      ex
    end

    def execute_unfiltered_count!
      opts = { size: 0 }

      active_record_klass.__elasticsearch__.search(opts).total_entries
    end
  end
end
