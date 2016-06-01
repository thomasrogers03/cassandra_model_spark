module CassandraModel
  module Spark
    class KafkaReactor < ::BatchReactor::ReactorCluster

      def initialize(size, options = {})
        seed_brokers = options.delete(:seed_brokers)
        producer_options = {
            broker_list: seed_brokers,
            'serializer.class' => 'kafka.serializer.StringEncoder'
        }
        @producer = Kafka::Producer.new(producer_options)
        @producer.connect
        define_partitioner { |partition| partition % size }
        super(size, options, &method(:create_batch_callback))
      end

      def stop
        super.then do |result|
          @producer.close
          result
        end
      end

      private

      def create_batch_callback(_, &block)
        begin
          batch = KafkaBatch.new
          block[batch]
          Ione::Future.resolved(@producer.send_msgs(batch.messages))
        rescue Exception => e
          Ione::Future.failed(e)
        end
      end

    end
  end
end