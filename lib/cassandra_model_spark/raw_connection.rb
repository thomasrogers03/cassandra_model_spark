module CassandraModel
  class RawConnection
    def java_spark_context
      @spark_context ||= begin
        conf = SparkConf.new(true)
        conf.set('spark.app.name', 'cassandra_model_spark')
        conf.set('spark.master', 'local[*]')
        conf.set('spark.cassandra.connection.host', config[:hosts].first)
        JavaSparkContext.new(conf)
      end
    end

    def spark_context
      java_spark_context.sc
    end
  end
end