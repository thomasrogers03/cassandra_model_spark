package org.apache.spark.api.cassandra_model

import scala.collection.mutable._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql.types._

object MapStringStringRowMapping {
  private def canDecode(blob: Array[Byte]) = {
    new String(blob.slice(0, 4)) == "MRSH"
  }

  private def decodeValue(blob: Array[Byte]): AnyRef = {
    if (canDecode(blob)) {
      val decoder = new MarshalLoader(blob)
      val value = decoder.getValue()

      value match {
        case (m: Map[_, _]) => m map {
          case (key, value) => {
            val new_value = value match {
              case Some(some) => String.valueOf(some)
              case None => null
            }
            (String.valueOf(key), new_value)
          }
        }
        case _ => new IllegalArgumentException("Unsupported Ruby Type")
      }
    } else {
      blob
    }
  }

  private def updatedRow(row: CassandraRow): CassandraRow = {
    val columns = row.columnNames
    val values = row.columnValues.map{
      value => value match {
        case (blob: Array[Byte]) => decodeValue(blob)
        case _ => value
      }
    }

    new CassandraRow(columns, values)
  }

  def mappedRDD(rdd: RDD[CassandraRow]): RDD[CassandraRow] = {
    rdd.map(
      row => updatedRow(row)
    )
  }
}

object SparkRowRowMapping {
  private def canDecode(blob: Array[Byte]) = {
    new String(blob.slice(0, 4)) == "MRSH"
  }

  private def decodeValue(blob: Array[Byte]): AnyRef = {
    if (canDecode(blob)) {
      val decoder = new MarshalLoader(blob)
      val value = decoder.getValue()

      value match {
        case (m: Map[_, _]) => Row.fromSeq(m.values.toSeq)
        case (a: Array[_]) => Row.fromSeq(a.toSeq)
        case _ => new IllegalArgumentException("Unsupported Ruby Type")
      }
    } else {
      blob
    }
  }

  private def updatedRow(row: CassandraRow): CassandraRow = {
    val columns = row.columnNames
    val values = row.columnValues.map{
      value => value match {
        case (blob: Array[Byte]) => decodeValue(blob)
        case _ => value
      }
    }

    new CassandraRow(columns, values)
  }

  def mappedRDD(rdd: RDD[CassandraRow]): RDD[CassandraRow] = {
    rdd.map(
      row => updatedRow(row)
    )
  }
}
