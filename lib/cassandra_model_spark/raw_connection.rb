module CassandraModel
  class RawConnection
    def java_spark_context
      @spark_context ||= begin
        conf = SparkConf.new(true)
        conf.set('spark.app.name', 'cassandra_model_spark')
        conf.set('spark.master', 'local[*]')
        conf.set('spark.cassandra.connection.host', config[:hosts].first)
        flat_spark_config.each { |key, value| conf.set(key, value) }
        JavaSparkContext.new(conf)
      end
    end

    def spark_context
      java_spark_context.sc
    end

    private

    def flat_spark_config(config = spark_config)
      config.inject({}) do |memo, (key, value)|
        if value.is_a?(Hash)
          child_conf = flat_spark_config(value)
          child_conf.each do |child_key, child_value|
            memo["#{key}.#{child_key}"] = child_value
          end
          memo
        else
          memo.merge!(key.to_s => value)
        end
      end
    end

    def spark_config
      config.slice(:spark)
    end
  end
end