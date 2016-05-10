module CassandraModel
  module Spark
    class Schema
      attr_reader :schema

      def initialize(sql_schema)
        @schema = sql_schema.fields.inject({}) do |memo, field|
          column = field.name
          type = field.dataType
          memo.merge!(column.to_sym => sql_type(type))
        end
      end

      def ==(rhs)
        rhs.is_a?(Schema) && rhs.schema == schema
      end

      private

      def sql_type(type)
        case sql_type_name(type)
          when 'ArrayType'
            [:list, sql_type(type.elementType)]
          when 'MapType'
            [:map, sql_type(type.keyType), sql_type(type.valueType) ]
          when 'IntegerType'
            :int
          when 'BooleanType'
            :boolean
          when 'DoubleType'
            :double
          when 'BinaryType'
            :blob
          when 'TimestampType'
            :timestamp
          else
            :text
        end
      end

      def sql_type_name(data_type)
        data_type.getClass.getSimpleName
      end

    end
  end
end
