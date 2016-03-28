package org.apache.spark.api.cassandra_model

import org.apache.spark.rdd._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql._

object RowConversions {
  def cassandraRDDToRowRDD(rdd: RDD[CassandraRow]): RDD[Row] = {
    rdd.map(row => Row.fromSeq(cassandraToRow(row)))
  }

  private def cassandraToRow(row: CassandraRow): Seq[Any] = {
    row.columnValues.map {
      case (date: java.util.Date) => new java.sql.Timestamp(date.getTime())
      case (uuid: java.util.UUID) => uuid.toString()
      case value => value
    }
  }
}
