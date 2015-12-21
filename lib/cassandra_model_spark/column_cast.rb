module CassandraModel
  module Spark
    class ColumnCast

      def initialize(key, type)
        @key = key
        @type = type.to_s.upcase
      end

      def quote(quote)
        "CAST(#{quote}#{@key}#{quote} AS #{@type})"
      end

    end
  end
end
