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

  def read
  end

  def createDataFrame(rdd, schema)
    SqlDataFrame.new(self, rdd, schema)
  end

  protected

  attr_reader :spark_context
end

class SqlDataFrame
  attr_reader :sql_context, :rdd, :schema

  def self.create_schema(schema_hash)
    fields = schema_hash.map { |name, type| SqlStructField.apply(name, type, true, SqlMetadata.empty) }
    SqlStructType.new(fields)
  end

  def initialize(sql_context, rdd, schema)
    @sql_context = sql_context
    @rdd = rdd
    @schema = schema.is_a?(SqlStructType) ? schema : create_schema(schema)
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

  def write
    raise NotImplementedError.new
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

class SqlDataTypeClass < Struct.new(:simple_name)
  alias :getSimpleName :simple_name
end

#noinspection RubyClassMethodNamingConvention,RubyInstanceMethodNamingConvention
class SqlDataType
  def self.getClass
    SqlDataTypeClass.new(to_s.match(/^Sql(.+)$/)[1])
  end

  def self.toString
    to_s.match(/^Sql(.+)$/)[1]
  end

  def getClass
    self.class.getClass
  end
end

class SqlFakeType < SqlDataType
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
class SqlMetadata < SqlDataType
  EMPTY = new.freeze

  def self.empty
    EMPTY
  end
end
class SqlNullType < SqlDataType
end
class SqlPrecisionInfo < SqlDataType
end
class SqlShortType < SqlDataType
end
class SqlStringType < SqlDataType
end
class SqlArrayType < SqlDataType
  attr_reader :elementType, :containsNull

  def self.apply(element_type)
    new(element_type)
  end

  def initialize(element_type)
    @elementType = element_type
    @containsNull = true
  end

  def toString
    "ArrayType(#{elementType.toString},#{!!containsNull})"
  end

  def ==(rhs)
    rhs.is_a?(SqlArrayType) &&
        elementType == rhs.elementType &&
        containsNull == rhs.containsNull
  end
end
class SqlMapType < SqlDataType
  attr_reader :keyType, :valueType, :valueContainsNull

  def self.apply(key_type, value_type, value_contains_null)
    new(key_type, value_type, value_contains_null)
  end

  def initialize(key_type, value_type, value_contains_null)
    @keyType = key_type
    @valueType = value_type
    @valueContainsNull = value_contains_null
  end

  def toString
    "MapType(#{keyType.toString},#{valueType.toString},#{!!valueContainsNull})"
  end

  def ==(rhs)
    rhs.is_a?(SqlMapType) &&
        keyType == rhs.keyType &&
        valueType == rhs.valueType &&
        valueContainsNull == rhs.valueContainsNull
  end
end
class SqlStructField < SqlDataType
  attr_reader :name, :dataType, :nullable, :metadata

  def self.apply(name, data_type, contains_null, meta_data)
    new(name, data_type, contains_null, meta_data)
  end

  def initialize(name, data_type, contains_null, meta_data)
    @name = name
    @dataType = data_type
    @nullable = contains_null
    @metadata = meta_data
  end

  def ==(rhs)
    rhs.is_a?(SqlStructField) &&
        name == rhs.name &&
        dataType == rhs.dataType &&
        nullable == rhs.nullable &&
        metadata == rhs.metadata
  end
end
class SqlStructType < SqlDataType
  attr_reader :fields

  def self.apply(fields)
    new(fields)
  end

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

  def toString
    @internal_type.toString
  end
end

class SqlRowConversions
  def self.cassandraRDDToRowRDD(rdd)
    MappedRDD.new(rdd)
  end

  MappedRDD = Struct.new(:rdd)
end