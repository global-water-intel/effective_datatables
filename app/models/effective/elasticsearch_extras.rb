module Effective
  module ElasticsearchExtras
    extend ActiveSupport::Concern

    included do
    end

    module ClassMethods
      def synonyms
        []
      end
    end
  end
end
