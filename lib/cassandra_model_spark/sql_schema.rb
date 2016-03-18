module CassandraModel
  module Spark
    class SqlSchema
      attr_reader :schema

      def initialize(cassandra_schema)
        fields = cassandra_schema.map do |column, type|
          SqlStructField.apply(column.to_s, sql_type(type), true, nil)
        end
        @schema = SqlStructType.apply(fields)
      end

      def sql_type(type)
        case type
          when Array
            base_type, first_type, second_type = type
            case base_type
              when :map
                SqlMapType.apply(sql_type(first_type), sql_type(second_type), true)
              else
                SqlArrayType.apply(sql_type(first_type), true)
            end
          when :int
            SqlIntegerType
          when :double
            SqlDoubleType
          when :blob
            SqlBinaryType
          when :timestamp
            SqlTimestampType
          else
            SqlStringType
        end
      end

    end
  end
end
