module CassandraModel
  module Spark
    class ColumnCast

      def initialize(key, type)
        @key = key
        @type = type.to_s.upcase
      end

      def quote(quote)
        quoted_key = if @key.respond_to?(:quote)
                       @key.quote(quote)
                     else
                       "#{quote}#{@key}#{quote}"
                     end
        "CAST(#{quoted_key} AS #{@type})"
      end

    end
  end
end

class Symbol
  def cast_as(type)
    CassandraModel::Spark::ColumnCast.new(self, type)
  end
end
