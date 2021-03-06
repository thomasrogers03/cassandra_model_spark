module CassandraModel
  module Spark
    class Application
      extend Forwardable

      def_delegator :@java_spark_context, :value!, :java_spark_context
      attr_accessor :spark_config

      def initialize(config)
        @spark_config = config || {}
        @java_spark_context = Concurrent::Delay.new do
          Spark::Lib::JavaSparkContext.new(spark_conf).tap do |java_spark_context|
            java_spark_context.sc.addJar("#{Spark.classpath}/cmodel_scala_helper.jar")
          end
        end
      end

      def spark_context
        java_spark_context.sc
      end

      def has_spark_context?
        @java_spark_context.fulfilled?
      end

      #noinspection RubyInstanceMethodNamingConvention
      def create_java_spark_streaming_context
        Spark::Lib::JavaSparkStreamingContext.new(java_spark_context, Spark::Lib::SparkDuration.new(2000))
      end

      private

      def spark_conf
        Spark::Lib::SparkConf.new(true).tap do |conf|
          conf.set('spark.app.name', 'cassandra_model_spark')
          conf.set('spark.master', 'local[*]')
          conf.set('spark.cassandra.connection.host', 'localhost')
          flat_spark_config.each { |key, value| conf.set(key, value) }
        end
      end

      def flat_spark_config(config = {spark: spark_config})
        config.inject({}) do |memo, (key, value)|
          if value.is_a?(Hash)
            memo.merge!(child_spark_conf(key, value))
          else
            memo.merge!(key.to_s => value)
          end
        end
      end

      def child_spark_conf(key, value)
        child_conf = flat_spark_config(value)
        child_conf.inject({}) do |child_memo, (child_key, child_value)|
          child_memo.merge!("#{key}.#{child_key}" => child_value)
        end
      end
    end
  end
end
