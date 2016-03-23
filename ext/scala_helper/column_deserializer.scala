package org.apache.spark.api.cassandra_model

import scala.collection.mutable._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql.types._

object ColumnDeserializer {
  private def canDecode(blob: Array[Byte]): Boolean = {
    new String(blob.slice(0, 4)) == "MRSH"
  }

  private def decodeValue(blob: Array[Byte]): AnyRef = {
    if (canDecode(blob)) {
      val decoder = new MarshalLoader(blob)
      decoder.getValue()
    } else {
      throw new IllegalArgumentException("Unsupported serialized column")
    }
  }

  privet def updatedRow(row: CassandraRow, column: Int): CassandraRow = {
    val columns = row.columnNames
    val updated_value = row.columnValues(column) match {
      case (blob: Array[Byte]) => decodeValue(blob)
    }
    val values = row.columnValues.updated(column, updated_value)

    new CassandraRow(columns, values)
  }

  def mappedRDD(rdd: RDD[CassandraRow], column: Int): RDD[CassandraRow] = {
    rdd.map(updatedRow(_, column))
  }
}
