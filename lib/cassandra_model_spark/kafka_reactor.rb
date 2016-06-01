module CassandraModel
  module Spark
    class KafkaReactor < ::BatchReactor::ReactorCluster

      def initialize(size, options = {})
        seed_brokers = options.delete(:seed_brokers)
        @kafka_pool = ConnectionPool.new(size: size) do
          Kafka.new(:seed_brokers => seed_brokers)
        end
        define_partitioner { |partition| partition % size }
        super(size, options, &method(:create_batch_callback))
      end

      private

      def create_batch_callback(_, &block)
        begin
          @kafka_pool.with do |kafka|
            producer = kafka.producer
            block[producer]
            Ione::Future.resolved(producer.deliver_messages)
          end
        rescue Exception => e
          Ione::Future.failed(e)
        end
      end

    end
  end
end