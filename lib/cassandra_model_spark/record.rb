module CassandraModel
  class Record
    def self.rdd
      @spark_rdd ||= Spark::Lib::SparkCassandraHelper.cassandraTable(
          table.connection.spark_context,
          table.connection.config[:keyspace],
          table_name)
    end

    def self.rdd_row_mapping
      nil
    end

    def self.count
      rdd.count
    end

    def self.sql_schema
      Spark::SqlSchema.new(cassandra_columns)
    end
  end
end