import_java_object 'java.util.ArrayList'
import_java_object 'org.apache.spark.SparkConf'
import_java_object 'org.apache.spark.api.java.JavaSparkContext'
import_java_object 'org.apache.spark.streaming.api.java.JavaStreamingContext', as: 'JavaSparkStreamingContext'
import_java_object 'org.apache.spark.streaming.Duration', as: 'SparkDuration'
import_java_object 'org.apache.spark.sql.cassandra.CassandraSQLContext'
import_java_object 'java.util.HashMap', as: 'JavaHashMap'
import_java_object 'org.apache.spark.sql.SQLContext', as: 'SparkSQLContext'
import_java_object 'org.apache.spark.sql.RowFactory', as: 'SparkRowFactory'
import_java_object 'org.apache.spark.sql.catalyst.expressions.GenericRow', as: 'SqlGenericRow'
import_java_object 'org.apache.log4j.Logger', as: 'JLogger'
import_java_object 'org.apache.log4j.Level', as: 'JLevel'
import_java_object 'org.apache.log4j.Priority', as: 'JPriority'
import_java_object 'org.apache.spark.util.Utils', as: 'SparkUtils'
import_java_object 'org.apache.spark.storage.StorageLevel', as: 'JStorageLevel'

import_java_object 'org.apache.spark.api.cassandra_model.ColumnDeserializer', as: 'SparkColumnDeserializer'
import_java_object 'org.apache.spark.api.cassandra_model.RowConversions', as: 'SqlRowConversions'
import_java_object 'org.apache.spark.api.cassandra_model.CassandraHelper', as: 'SparkCassandraHelper'
import_java_object 'org.apache.spark.api.cassandra_model.RowCounter'
import_java_object 'org.apache.spark.api.cassandra_model.DataTypeHelper', as: 'SparkSqlDataTypeHelper'
import_java_object 'org.apache.spark.api.cassandra_model.MarshalLoader', as: 'ScalaMarshalLoader'
import_java_object 'org.apache.spark.api.cassandra_model.MapStringStringRowMapping', as: 'SparkMapStringStringRowMapping'
import_java_object 'org.apache.spark.api.cassandra_model.EncodeBytesRowMapping', as: 'SparkEncodeBytesRowMapping'
import_java_object 'org.apache.spark.api.cassandra_model.DecodeBytesRowMapping', as: 'SparkDecodeBytesRowMapping'
import_java_object 'org.apache.spark.api.cassandra_model.SparkRowRowMapping', as: 'SparkSparkRowRowMapping'
import_java_object 'org.apache.spark.api.cassandra_model.MappedColumnRowMapping', as: 'SparkMappedColumnRowMapping'
import_java_object 'org.apache.spark.api.cassandra_model.LuaRDD'

import_java_object 'org.apache.spark.deploy.master.Master', as: 'SparkMaster'
import_java_object 'org.apache.spark.deploy.worker.RubyWorkerStarter', as: 'SparkWorkerStarter'

if CassandraModel.const_defined?('TESTING_SCALA')
  import_java_object 'com.datastax.spark.connector.CassandraRow', as: 'SparkCassandraRow'
  import_java_object 'org.apache.spark.api.cassandra_model.LuaRowValue'
  import_java_object 'org.apache.spark.api.cassandra_model.LuaRowLib'
end

%w(BinaryType BooleanType ByteType DataType
   DateType Decimal DecimalType DoubleType FloatType IntegerType
   LongType Metadata NullType PrecisionInfo ShortType
   ArrayType MapType
   StringType StructField StructType TimestampType).each do |sql_type|
  type = import_quiet { CassandraModel::Spark::Lib::SparkSqlDataTypeHelper.public_send(:"get#{sql_type}") }
  CassandraModel::Spark::Lib.const_set(:"Sql#{sql_type}", type)
end

#noinspection RubyConstantNamingConvention
SqlStringArrayType = CassandraModel::Spark::Lib::SparkSqlDataTypeHelper.getArrayType(CassandraModel::Spark::Lib::SqlStringType)

#noinspection RubyConstantNamingConvention
SqlIntegerArrayType = CassandraModel::Spark::Lib::SparkSqlDataTypeHelper.getArrayType(CassandraModel::Spark::Lib::SqlIntegerType)

#noinspection RubyConstantNamingConvention
SqlStringStringMapType = CassandraModel::Spark::Lib::SparkSqlDataTypeHelper.getMapType(CassandraModel::Spark::Lib::SqlStringType, CassandraModel::Spark::Lib::SqlStringType)
