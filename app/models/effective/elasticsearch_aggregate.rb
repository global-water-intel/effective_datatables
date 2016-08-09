module Effective
  class ElasticsearchAggregate
    attr_accessor :field, :type, :options

    def initialize(field, type, options)
      self.field = field
      self.type = type
      self.options = options
    end

    def key
      agg_key
    end

    def body
      base = {}
      case type
      when :date
        base[key] = {
          date_histogram: {
            field: field,
            interval: options[:interval]
          }
        }

        # if options.has_key?(:sum_over)
        #   search_options[:aggs][key][:aggs] = {
        #     "#{key}-2" => {
        #       sum: {
        #         field: options[:sum_over]
        #       }
        #     }
        #   }
        # end

        # if options.has_key?(:filter)
        #   old_search = search_options[:aggs].delete(key)


        #   search_options[:aggs][key][:filter] = {
        #     term: options[:filter]
        #   }
        # end
      when :count
        base[key] = {
          terms: {
            field: field,
            size: 0
          }
        }
      end
    end

    def results_as_hash_from(aggregations)
      raw = aggregations[key]['buckets']

      case type
      when :date
        Hash[raw.map { |h| [h['key_as_string'], h['doc_count']] }]
      else
        Hash[raw.map { |h| [h['key'], h['doc_count']] }]
      end
    end

    private

    def hasher
      @hasher ||= Digest::SHA1.new
    end

    def agg_key
      hasher.hexdigest "#{field}_#{type}_#{options}".downcase
    end
  end
end
