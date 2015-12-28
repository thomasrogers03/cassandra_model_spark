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
    rhs.is_a?(CassandraSQLContext) && spark_context == rhs.spark_context
  end

  def setKeyspace(value)
    @keyspace = value
  end

  def sql(*_)
  end

  protected

  attr_reader :spark_context
end

class SqlDataFrame
  attr_reader :sql_context, :rdd, :schema

  def self.create_schema(schema_hash)
    fields = schema_hash.map { |name, type| SqlStructField.new(name, type) }
    SqlStructType.new(fields)
  end

  def initialize(sql_context, rdd, schema)
    @sql_context = sql_context
    @rdd = rdd
    @schema = create_schema(schema)
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

  #noinspection RubyUnusedLocalVariable
  def register_temp_table(name)
  end

  def ==(rhs)
    rhs.is_a?(SqlDataFrame) &&
        sql_context == rhs.sql_context &&
        rdd == rhs.rdd &&
        schema == rhs.schema
  end

  private

  def create_schema(schema)
    self.class.create_schema(schema)
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

class SqlDataTypeClass < Struct.new(:simple_name)
  alias :getSimpleName :simple_name
end

#noinspection RubyClassMethodNamingConvention,RubyInstanceMethodNamingConvention
class SqlDataType
  def self.getClass
    SqlDataTypeClass.new(to_s.match(/^Sql(.+)$/)[1])
  end

  def self.to_string
    to_s.match(/^Sql(.+)$/)[1]
  end

  def getClass
    self.class.getClass
  end
end

class SqlFakeType < SqlDataType
end
class SqlStringArrayType < SqlDataType
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
class SqlStringStringMapType < SqlDataType
  def self.to_s
    'SqlMapType(StringType,StringType,true)'
  end
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
  attr_reader :name, :type

  def initialize(name, type)
    @name = name
    @type = type
  end

  def data_type
    @type
  end

  def ==(rhs)
    rhs.is_a?(SqlStructField) &&
        name == rhs.name &&
        type == rhs.type
  end
end
class SqlStructType < SqlDataType
  attr_reader :fields

  def initialize(fields)
    @fields = fields
  end

  def ==(rhs)
    rhs.is_a?(SqlStructType) && fields == rhs.fields
  end
end
class SqlTimestampType < SqlDataType
end
class SqlTypeWrapper < SqlDataType
  def initialize(internal_type)
    @internal_type = internal_type
  end

  def to_string
    @internal_type.to_string
  end
end
