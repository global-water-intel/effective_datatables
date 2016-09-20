module Effective
  class ElasticsearchAggregate
    attr_accessor :type, :field, :options

    def initialize(type, field, options)
      self.type = type
      self.field = field
      self.options = options.presence || {}
    end

    def key
      agg_key
    end

    def body
      @body = {}

      case type
      when :date_histogram_count, :date_histogram_sum_with_filter
        @body[key] = {
          date_histogram: {
            field: field,
            interval: options[:interval]
          }
        }

        if type == :date_histogram_sum_with_filter
          @body[key][:aggs] = {
            key_2 => {
              stats: { field: options[:sum_over] }
            }
          }

          old_body = @body.delete(key)

          @body[key] = {
            aggs: { key_3 => old_body }
          }

          @body[key][:filter] = if options[:filter].present?
                                  {
                                    term: options[:filter]
                                  }
                                else
                                  {
                                    exists: { field: 'id' }
                                  }
                                end
        end
      when :terms_count
        @body[key] = {
          terms: {
            field: field,
            size: 0
          }
        }
      when :filtered_terms_sum
        filter_with_default = if options[:filter].present?
                                {
                                  term: options[:filter]
                                }
                              else
                                {
                                  exists: { field: 'id' }
                                }
                              end
        @body[key] = {
          filter: filter_with_default,
          aggs: {
            key_3 => {
              terms: { field: field, size: 0 },
              aggs: {
                key_2 => {
                  stats: { field: options[:sum_over] }
                }
              }
            }
          }
        }
      when :filtered_terms_multi_sum
        sum_over = Array(options[:sum_over])
        sum_over_aggs = {}
        sum_over.each do |so|
          sum_over_aggs[so] = { stats: { field: so } }
        end

        filter_with_default = if options[:filter].present?
                                {
                                  term: options[:filter]
                                }
                              else
                                {
                                  exists: { field: 'id' }
                                }
                              end
        @body[key] = {
          filter: filter_with_default,
          aggs: {
            key_3 => {
              terms: { field: field, size: 0 },
              aggs: sum_over_aggs
            }
          }
        }
      when :raw
        @body[key] = options[:raw_aggs]
      else
        binding.pry
      end

      @body
    end

    def results_as_hash_from(aggregations)
      case type
      when :date_histogram_count
        raw = aggregations[key]['buckets']
        Hash[raw.map { |h| [h['key_as_string'], h['doc_count']] }]
      when :terms_count
        raw = aggregations[key]['buckets']
        # binding.pry
        Hash[raw.map { |h| [h['key'], h['doc_count']] }]
      when :date_histogram_sum_with_filter
        raw = aggregations[key][key_3]['buckets']
        Hash[raw.map { |h| [h['key_as_string'], h[key_2]['sum']] }]
      when :filtered_terms_sum
        raw = aggregations[key][key_3]['buckets']

        Hash[raw.map { |h| [h['key'], h[key_2]['sum']] }]
      when :filtered_terms_multi_sum
        raw = aggregations[key][key_3]['buckets']

        arrays = raw.map do |h|
          val_hash = Hash[h.except('key', 'doc_count').map { |k, v| [k, v['sum']] }]
          [h['key'], val_hash]
        end
# binding.pry

        Hash[arrays]
      when :raw
        aggregations[key]
      else
        binding.pry
      end
    end

    def register_options(query_hash)
      query_hash.merge body
    end

    private

    def key_2
      "#{key}-2"
    end

    def key_3
      "#{key}-3"
    end

    def key_4
      "#{key}-4"
    end

    def hasher
      @hasher ||= Digest::SHA1.new
    end

    def agg_key
      @agg_key ||= "#{field}-#{type}-#{hasher.hexdigest("#{field}_#{type}_#{options}".downcase)}"
    end
  end
end
