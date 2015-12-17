class MockRecord < Struct.new(:attributes)
end

class SqlLong < Struct.new(:value)
  def to_i
    value.to_i
  end
end

class RDDRow < Hash
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
  #noinspection RubyUnusedLocalVariable
  def self.cassandra_table(context, keyspace, table)
    RDD.new(context)
  end
end
