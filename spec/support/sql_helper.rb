class JavaHash < Hash
end

class Hash
  def to_java
    JavaHash[self]
  end
end

class FilteredRDD
  attr_reader :rdd, :restriction

  def initialize(rdd, restriction)
    @rdd = rdd
    @restriction = restriction
  end
end

class SparkCassandraHelper
  #noinspection RubyClassMethodNamingConvention
  def self.filterRDD(rdd, restriction)
    FilteredRDD.new(rdd, restriction)
  end
end

class CassandraSQLContext
  attr_reader :keyspace

  def initialize(spark_context)
    @spark_context = spark_context
  end

  def ==(rhs)
    rhs.is_a?(CassandraSQLContext) && sql_context == rhs.sql_context
  end

  def setKeyspace(value)
    @keyspace = value
  end

  def sql
  end

  protected

  attr_reader :sql_context
end

class SqlDataFrame
  attr_reader :sql_context, :rdd, :schema

  def initialize(sql_context, rdd, schema)
    @sql_context = sql_context
    @rdd = rdd
    @schema = schema
  end

  def cache
    @cached = true
  end

  def unpersist
    @cached = false
  end

  def cached?
    !!@cached
  end

  def register_temp_table(name)
  end
end

class SparkSchemaBuilder
  def add_column(name, type)
    columns[name] = type
  end

  def columns
    @columns ||= {}
  end

  def create_data_frame(sql_context, rdd)
    SqlDataFrame.new(sql_context, rdd, columns)
  end
end

class SqlDataType
end

class SqlArrayType < SqlDataType
end
class SqlBinaryType < SqlDataType
end
class SqlBooleanType < SqlDataType
end
class SqlByteType < SqlDataType
end
class SqlDateType < SqlDataType
end
class SqlDecimal < SqlDataType
end
class SqlDecimalType < SqlDataType
end
class SqlDoubleType < SqlDataType
end
class SqlFloatType < SqlDataType
end
class SqlIntegerType < SqlDataType
end
class SqlLongType < SqlDataType
end
class SqlMapType < SqlDataType
end
class SqlMetadata < SqlDataType
end
class SqlNullType < SqlDataType
end
class SqlPrecisionInfo < SqlDataType
end
class SqlShortType < SqlDataType
end
class SqlStringType < SqlDataType
end
class SqlStructField < SqlDataType
end
class SqlStructType < SqlDataType
end
class SqlTimestampType < SqlDataType
end
