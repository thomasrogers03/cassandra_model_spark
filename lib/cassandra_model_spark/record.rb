module CassandraModel
  class Record
    def self.rdd
      @spark_rdd ||= SparkCassandraHelper.cassandra_table(
          table.connection.spark_context,
          table.connection.config[:keyspace],
          table_name)
    end

    def self.count
      rdd.count
    end
  end
end