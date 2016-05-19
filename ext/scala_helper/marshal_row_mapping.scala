package org.apache.spark.api.cassandra_model

import scala.collection.mutable._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql.types._
import javax.xml.bind.DatatypeConverter
import scala.collection.JavaConversions.mapAsScalaMap

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
            val new_value = Option(value) match {
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
    val values = row.columnValues.map {
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

object EncodeBytesRowMapping {
  private def updatedRow(row: CassandraRow): CassandraRow = {
    val columns = row.columnNames
    val values = row.columnValues.map {
      value => value match {
        case (blob: Array[Byte]) => "BS64" + DatatypeConverter.printBase64Binary(blob)
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

object DecodeBytesRowMapping {
  private def canDecode(str: String) = {
    str.substring(0, 3) == "BS64"
  }

  private def updatedRow(row: CassandraRow): CassandraRow = {
    val columns = row.columnNames
    val values = row.columnValues.map {
      value => value match {
        case str: String => {
          if (canDecode(str))
            DatatypeConverter.parseBase64Binary(str.substring(4))
          else
            str
        }
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
    val values = row.columnValues.map {
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

class MappedColumnRowMapping(val column_map: java.util.HashMap[String, String]) extends Serializable {
  private def updatedRow(row: CassandraRow): CassandraRow = {
    val columns = column_map.map {
      column_pair: (String, String) =>
        column_pair._2
    }.toIndexedSeq
    val old_column_indices = column_map.map {
      column_pair: (String, String) =>
        row.columnNames.indexOf(column_pair._1)
    }
    val values = old_column_indices.map {
      case -1 => null
      case column_index => row.columnValues.apply(column_index)
    }.toIndexedSeq

    new CassandraRow(columns, values)
  }

  def mappedRDD(rdd: RDD[CassandraRow]): RDD[CassandraRow] = {
    rdd.map(
      row => updatedRow(row)
    )
  }
}
