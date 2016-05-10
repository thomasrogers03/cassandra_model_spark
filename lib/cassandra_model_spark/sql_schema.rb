module CassandraModel
  module Spark
    class SqlSchema
      attr_reader :schema

      def initialize(cassandra_schema)
        fields = cassandra_schema.map do |column, type|
          Lib::SqlStructField.apply(column.to_s, sql_type(type), true, Lib::SqlMetadata.empty)
        end
        if RUBY_ENGINE == 'jruby'
          fields = fields.to_java('org.apache.spark.sql.types.StructField')
        end
        @schema = Lib::SqlStructType.apply(fields)
      end

      def ==(rhs)
        rhs.is_a?(SqlSchema) && rhs.schema == schema
      end

      private

      def sql_type(type)
        case type
          when Array
            base_type, first_type, second_type = type
            case base_type
              when :map
                Lib::SqlMapType.apply(sql_type(first_type), sql_type(second_type), true)
              else
                Lib::SqlArrayType.apply(sql_type(first_type))
            end
          when :int
            Lib::SqlIntegerType
          when :boolean
            Lib::SqlBooleanType
          when :double
            Lib::SqlDoubleType
          when :blob
            Lib::SqlBinaryType
          when :timestamp
            Lib::SqlTimestampType
          else
            Lib::SqlStringType
        end
      end

    end
  end
end
