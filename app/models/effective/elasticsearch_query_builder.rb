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
      when :string, :text
        query wildcard: { name => "*#{term.downcase}*" }
        # query match: { name => term }
      when :foreign_key
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

    def aggregate_for(field, type, option = nil)
      key = agg_key(field, type, option)

      raw = aggregations[key]['buckets']

      case type
      when :date
        Hash[raw.map { |h| [h['key_as_string'], h['doc_count']] }]
      else
        Hash[raw.map { |h| [h['key'], h['doc_count']] }]
      end
    end

    def add_aggregate(field, type, option = nil)
      key = agg_key(field, type, option)

      case type
      when :date
        search_options[:aggs][key] = {
          date_histogram: {
            field: field,
            interval: option
          }
        }
      when :count
        search_options[:aggs][key] = {
          terms: {
            field: field,
            size: 0
          }
        }
      end
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

    def agg_key(field, type, option)
      "#{field}_#{type}_#{option}".downcase
    end

    def response
      execute!
    end

    def execute!
      return @execute_result if @cache_execute && defined?(@execute_result)

      ex = active_record_klass.__elasticsearch__.search(search_options).page(page_number).per_page(page_size)

      return @execute_result = ex if @cache_execute
      ex
    end

    def execute_unfiltered_count!
      opts = { size: 0 }
      # binding.pry
      active_record_klass.__elasticsearch__.search(opts).total_entries
    end
  end
end
