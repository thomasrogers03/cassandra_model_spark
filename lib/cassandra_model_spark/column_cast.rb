module CassandraModel
  module Spark
    class ColumnCast
      include ThomasUtils::SymbolHelpers

      attr_reader :key

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

      def new_key(key)
        self.class.new(key, @type)
      end

    end
  end
end

module ThomasUtils
  class KeyChild
    def cast_as(type)
      CassandraModel::Spark::ColumnCast.new(self, type)
    end
  end
end

class Symbol
  def cast_as(type)
    CassandraModel::Spark::ColumnCast.new(self, type)
  end
end
