class MockRecord < Struct.new(:attributes)
end

module CassandraModel
  module Spark
    module Lib
      class SqlLong < Struct.new(:value)
        def to_i
          value.to_i
        end
      end

      class ScalaSeq < Struct.new(:array)
      end

      class ScalaValueWrapper < Struct.new(:value)
        def toString
          value
        end
      end

      class ScalaMapPair < Struct.new(:_1, :_2)
      end

      class ScalaMap < Struct.new(:hash)
        def toSeq
          pairs = hash.map do |key, value|
            ScalaMapPair.new(ScalaValueWrapper.new(key), ScalaValueWrapper.new(value))
          end
          ScalaSeq.new(pairs)
        end
      end

      class RDDRow < Hash
        def get(column)
          values[column]
        end

        def getInt(column)
          values[column].to_i
        end

        def getLong(column)
          SqlLong.new(values[column].to_i)
        end

        def getDouble(column)
          values[column].to_f
        end

        def getString(column)
          values[column].to_s
        end

        def getTimestamp(column)
          Time.at(values[column].to_f)
        end

        def getMap(column)
          ScalaMap.new(values[column])
        end
      end

      class RDD
        extend Forwardable

        attr_reader :context, :values
        def_delegator :values, :hash

        def initialize(context, values = [])
          @context = context
          @values = values.map { |hash| RDDRow[hash] }
        end

        def ==(rhs)
          rhs.is_a?(RDD) && values == rhs.values
        end

        def eql?(rhs)
          self == rhs
        end
      end

      class SparkCassandraHelper
        def self.cassandraTable(context, keyspace, table)
          RDD.new(context)
        end

        def self.cassandraTableForHost(context, keyspace, table, host)
          RDD.new(context)
        end
      end
    end
  end
end
