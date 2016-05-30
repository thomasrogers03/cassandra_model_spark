module CassandraModel
  module Spark
    class KafkaBatch
      attr_reader :messages

      def initialize
        @messages = []
      end

      def produce(message, options)
        messages << [options[:topic], options[:key], message]
      end

    end
  end
end
