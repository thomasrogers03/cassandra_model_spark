package org.apache.spark.api.cassandra_model

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import java.util.UUID

object DataTypeHelper {
  def getArrayType(key_type: DataType) = ArrayType(key_type)
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
  def getMapType(key_type: DataType, value_type: DataType) = MapType(key_type, value_type)
  def getMetadata = Metadata
  def getNullType = NullType
  def getPrecisionInfo = PrecisionInfo
  def getShortType = ShortType
  def getStringType = StringType
  def getStructField = StructField
  def getStructType = StructType
  def getTimestampType = TimestampType
  def getUUIDType = UUIDType
  def getTimeUUIDType = TimeUUIDType

  def getUUIDFromRow(row: Row, index: Int) = {
    row.getAs[SqlUUID](index).uuid
  }

  def getTimeUUIDFromRow(row: Row, index: Int) = {
    row.getAs[SqlTimeUUID](index).uuid
  }
}
