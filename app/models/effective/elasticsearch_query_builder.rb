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
          bool: {
            must: [
              { match_all: {} }
            ]
          }
        },
        sort: []
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

      case table_column['type']
      when :string, :text
        query wildcard: { name => "*#{term.downcase}*" }
        # query match: { name => term }
      when :foreign_key
        query match: { name => term }
      when :datetime
        query match: { name => term }
      else
        name_for_searching = active_record_klass.field_name_for_search(name)
        query wildcard: { name_for_searching => "*#{term.to_s.downcase}*" }
        # query match: { name => term }
      end
    end

    def query(hash = {})
      search_options[:query][:bool][:must] << hash

      self
    end

    def filter(hash = {})
      raise NotImplementedError
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
      execute_count!
    end

    def unfiltered_total_entries
      execute_unfiltered_count!
    end

    private

    def response
      execute!
    end

    def execute!
      # binding.pry
      active_record_klass.__elasticsearch__.search(search_options).page(page_number).per_page(page_size)
    end

    def execute_count!
      opts = search_options.merge size: 0
      active_record_klass.__elasticsearch__.search(opts).total_entries
    end

    def execute_unfiltered_count!
      opts = { size: 0 }
      active_record_klass.__elasticsearch__.search(opts).total_entries
    end
  end
end
