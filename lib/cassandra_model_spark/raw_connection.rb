module CassandraModel
  class RawConnection
    def java_spark_context
      conf = SparkConf.new(true)
      conf.set('spark.app.name', 'cassandra_model_spark')
      conf.set('spark.master', 'local[*]')
      conf.set('spark.cassandra.connection.host', config[:hosts].first)
      JavaSparkContext.new(conf)
    end
  end
end