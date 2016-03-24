package org.apache.spark.api.cassandra_model

import scala.collection.mutable._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql.types._

object ColumnDeserializer {
  def mappedRDD(rdd: RDD[CassandraRow], column: Int): RDD[CassandraRow] = {
    rdd.map(updatedRow(_, column))
  }

  private def updatedRow(row: CassandraRow, column: Int): CassandraRow =
  {
    val columns = row.columnNames
    val updated_value = getDecodedValue(row, column)
    val values = row.columnValues.updated(column, updated_value)

    new CassandraRow(columns, values)
  }

  private def getDecodedValue(row: CassandraRow, column: Int): AnyRef = row.columnValues(column) match {
    case (blob: Array[Byte]) => decodeValue(blob)
  }

  private def decodeValue(blob: Array[Byte]): AnyRef = {
    new MarshalLoader(blob).getValue()
  }
}
