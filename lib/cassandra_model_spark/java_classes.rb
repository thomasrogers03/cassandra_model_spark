import_java_object 'java.util.ArrayList'
import_java_object 'org.apache.spark.SparkConf'
import_java_object 'org.apache.spark.api.java.JavaSparkContext'
import_java_object 'org.apache.spark.sql.cassandra.CassandraSQLContext'
import_java_object 'java.util.HashMap', as: 'JavaHashMap'
import_java_object 'org.apache.spark.sql.SQLContext', as: 'SparkSQLContext'
import_java_object 'org.apache.spark.sql.RowFactory', as: 'SparkRowFactory'
import_java_object 'org.apache.log4j.Logger', as: 'JLogger'
import_java_object 'org.apache.log4j.Level', as: 'JLevel'
import_java_object 'org.apache.log4j.Priority', as: 'JPriority'
import_java_object 'org.apache.spark.util.Utils', as: 'SparkUtils'
import_java_object 'org.apache.spark.storage.StorageLevel', as: 'JStorageLevel'
import_java_object 'org.apache.spark.api.cassandra_model.CassandraHelper', as: 'SparkCassandraHelper'
import_java_object 'org.apache.spark.api.cassandra_model.SchemaBuilder', as: 'SparkSchemaBuilder'
import_java_object 'org.apache.spark.api.cassandra_model.DataTypeHelper', as: 'SparkSqlDataTypeHelper'
import_java_object 'org.apache.spark.api.cassandra_model.MarshalLoader', as: 'ScalaMarshalLoader'
import_java_object 'org.apache.spark.api.cassandra_model.MapStringStringRowMapping', as: 'SparkMapStringStringRowMapping'
import_java_object 'org.apache.spark.api.cassandra_model.SparkRowRowMapping', as: 'SparkSparkRowRowMapping'
import_java_object 'org.apache.spark.deploy.master.Master', as: 'SparkMaster'
import_java_object 'org.apache.spark.deploy.worker.Worker', as: 'SparkWorker'

%w(BinaryType BooleanType ByteType DataType
   DateType Decimal DecimalType DoubleType FloatType IntegerType
   LongType Metadata NullType PrecisionInfo ShortType
   StringType StructField StructType TimestampType).each do |sql_type|
  Object.const_set(:"Sql#{sql_type}", import_quiet { SparkSqlDataTypeHelper.public_send(:"get#{sql_type}") })
end

#noinspection RubyConstantNamingConvention
SqlStringArrayType = SparkSqlDataTypeHelper.getArrayType(SqlStringType)

#noinspection RubyConstantNamingConvention
SqlIntegerArrayType = SparkSqlDataTypeHelper.getArrayType(SqlIntegerType)

#noinspection RubyConstantNamingConvention
SqlStringStringMapType = SparkSqlDataTypeHelper.getMapType(SqlStringType, SqlStringType)
