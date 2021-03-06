package org.apache.spark.api.cassandra_model

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import java.util.Date
import scala.collection.mutable.HashMap

object RowCounter {
  private def createDeserializedStream(stream: DStream[(String, String)]) = {
    stream.map { column_pair: (String, String) =>
      val deserialized_row = new MarshalLoader(column_pair._2.getBytes()).getValue.asInstanceOf[HashMap[AnyRef, AnyRef]]
      deserialized_row
    }
  }

  private def createFilteredStream(acceptable_columns: Array[String], counter_column: String, stream: DStream[HashMap[AnyRef, AnyRef]]) = {
    stream.map { row: HashMap[AnyRef, AnyRef] =>
      val values = acceptable_columns.map(row.getOrElse(_, null))
      val count: Int = row.getOrElse(counter_column, null) match {
        case null => 1
        case num: AnyRef => num.asInstanceOf[java.lang.Integer]
      }
      (values.toSeq, count)
    }
  }

  private def createReducedStream(stream: DStream[(Seq[AnyRef], Int)]) = {
    stream.reduceByKeyAndWindow((lhs: Int, rhs: Int) => lhs + rhs, Seconds(2), Seconds(2))
  }
}

class RowCounter(
                  val sc: SparkContext,
                  val columns: Array[String],
                  val timestamp_column: String,
                  val count_column: String,
                  val stream: DStream[(String, String)]
                  ) {
  private val deserialized_stream = RowCounter.createDeserializedStream(stream)
  private val filtered_stream = RowCounter.createFilteredStream(columns, count_column, deserialized_stream)
  private val reduced_stream = RowCounter.createReducedStream(filtered_stream)

  def saveToCassandra(keyspace: String, table: String, host: String) = {
    implicit val c = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", host))
    val updated_columns = columns :+ timestamp_column :+ count_column

    reduced_stream.foreachRDD { (rdd: RDD[(Seq[AnyRef], Int)], timestamp: Time) =>
      rdd.map { row: (Seq[AnyRef], Int) =>
        val count: java.lang.Integer = row._2
        val values = row._1 :+ new Date(timestamp.milliseconds) :+ count

        new CassandraRow(updated_columns, values.toIndexedSeq)
      }.saveToCassandra(keyspace, table)
    }
  }

  def print = reduced_stream.print

}