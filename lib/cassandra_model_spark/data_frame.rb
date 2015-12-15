module CassandraModel
  module Spark
    class DataFrame
      include QueryHelper

      SQL_TYPE_MAP = {
          int: SqlIntegerType,
          text: SqlStringType,
          double: SqlDoubleType,
          timestamp: SqlTimestampType,
      }.freeze

      def initialize(record_klass, rdd, options = {})
        @table_name = options.fetch(:alias) { record_klass.table_name }
        @sql_context = options[:sql_context]
        @record_klass = record_klass
        @rdd = rdd
      end

      def sql_context
        @sql_context ||= create_sql_context
      end

      def spark_data_frame
        @frame ||= SparkSchemaBuilder.new.tap do |builder|
          record_klass.cassandra_columns.each do |name, type|
            type = SQL_TYPE_MAP.fetch(type) { SqlStringType }
            builder.add_column(name.to_s, type)
          end
        end.create_data_frame(sql_context, rdd).tap { |frame| frame.register_temp_table(table_name) }
      end

      def cached(&block)
        spark_data_frame.cache
        instance_eval(&block)
        spark_data_frame.unpersist
      end

      private

      attr_reader :record_klass, :rdd, :table_name

      def create_sql_context
        CassandraSQLContext.new(record_klass.table.connection.spark_context).tap do |context|
          context.setKeyspace(record_klass.table.connection.config[:keyspace])
        end
      end
    end
  end
end
