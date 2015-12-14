module CassandraModel
  module Spark
    class DataFrame
      SQL_TYPE_MAP = {
          int: SqlIntegerType,
          text: SqlStringType,
          double: SqlDoubleType,
          timestamp: SqlTimestampType,
      }.freeze

      def initialize(record_klass, rdd)
        @record_klass = record_klass
        @rdd = rdd
      end

      def sql_context
        @sql_context ||= CassandraSQLContext.new(record_klass.table.connection.spark_context)
      end

      def spark_data_frame
        @frame ||= SparkSchemaBuilder.new.tap do |builder|
          record_klass.cassandra_columns.each do |name, type|
            type = SQL_TYPE_MAP.fetch(type) { SqlStringType }
            builder.add_column(name.to_s, type)
          end
        end.create_data_frame(sql_context, rdd)
      end

      private

      attr_reader :record_klass, :rdd
    end
  end
end
