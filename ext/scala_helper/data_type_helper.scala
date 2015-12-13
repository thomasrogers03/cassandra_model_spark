package org.apache.spark.api.cassandra_model

import org.apache.spark.sql.types._

object DataTypeHelper {
  def getArrayType = ArrayType
  def getBinaryType = BinaryType
  def getBooleanType = BooleanType
  def getByteType = ByteType
  def getDataType = DataType
  def getDateType = DateType
  def getDecimal = Decimal
  def getDecimalType = DecimalType
  def getDoubleType = DoubleType
  def getFloatType = FloatType
  def getIntegerType = IntegerType
  def getLongType = LongType
  def getMapType = MapType
  def getMetadata = Metadata
  def getNullType = NullType
  def getPrecisionInfo = PrecisionInfo
  def getShortType = ShortType
  def getStringType = StringType
  def getStructField = StructField
  def getStructType = StructType
  def getTimestampType = TimestampType
}