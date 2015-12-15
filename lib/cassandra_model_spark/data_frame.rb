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

      attr_reader :table_name

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

      def request_async(*_)
        ResultPaginator.new(first_async) {}
      end

      def first_async(*_)
        Cassandra::Future.error(NotImplementedError.new)
      end

      def query(restriction, options)
        select_columns = record_klass.select_columns(options.fetch(:select) { %w(*) }) * ', '
        where_clause = query_where_clause(restriction)
        sql_context.sql("SELECT #{select_columns} FROM #{table_name}#{where_clause}")
      end

      private

      attr_reader :record_klass, :rdd

      def create_sql_context
        CassandraSQLContext.new(record_klass.table.connection.spark_context).tap do |context|
          context.setKeyspace(record_klass.table.connection.config[:keyspace])
        end
      end

      def query_where_clause(restriction)
        if restriction.present?
          restriction_clause = restriction.map do |key, value|
            updated_key = key.is_a?(ThomasUtils::KeyComparer) ? key : "#{key} ="
            value = "'#{value}'" if value.is_a?(String) || value.is_a?(Time)
            "#{updated_key} #{value}"
          end * ' AND '
          " WHERE #{restriction_clause}"
        end
      end
    end
  end
end
