package org.apache.spark.api.cassandra_model

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import java.util.Date

object RowCounter {
  private def createDeserializedStream(stream: DStream[(String, String)]) = {
    stream.map { column_pair: (String, String) =>
      val deserialized_row = new MarshalLoader(column_pair._2.getBytes()).getValue match {
        case arr: Array[AnyRef] => arr
      }
      (deserialized_row.toSeq, 1)
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
                  val stream: DStream[(String, String)]
                  ) {
  private val deserialized_stream = RowCounter.createDeserializedStream(stream)
  private val reduced_stream = RowCounter.createReducedStream(deserialized_stream)

  def saveToCassandra(keyspace: String, table: String, host: String) = {
    implicit val c = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", host))
    val updated_columns = columns :+ timestamp_column

    reduced_stream.foreachRDD { (rdd: RDD[(Seq[AnyRef], Int)], timestamp: Time) =>
      rdd.map { row: (Seq[AnyRef], Int) =>
        val count: java.lang.Integer = row._2
        val values = row._1 :+ count :+ new Date(timestamp.milliseconds)

        new CassandraRow(updated_columns, values.toIndexedSeq)
      }.saveToCassandra(keyspace, table)
    }
  }

  def print = reduced_stream.print

}