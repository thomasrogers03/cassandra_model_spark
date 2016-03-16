package org.apache.spark.api.cassandra_model

import org.apache.spark.rdd._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

class SchemaBuilder {
  var fields = Array[StructField]()

  def build = StructType(fields)

  def cassandraRDDToRDD(rdd: RDD[CassandraRow]) = {
    rdd.map(
      p => Row.fromSeq(
        p.columnValues.map {
          p => p match {
            case (d: java.util.Date) => new java.sql.Timestamp(d.getTime())
            case (u: java.util.UUID) => u.toString()
            case _ => p
          }
        }
      )
    )
  }

  def createDataFrame(sqlc: SQLContext, rdd: RDD[CassandraRow]) = {
    val new_rdd = cassandraRDDToRDD(rdd)
    sqlc.createDataFrame(new_rdd, build)
  }

  def addColumn(name: String, sql_type: DataType) = {
    fields :+= StructField(name, sql_type, true)
  }

  def addColumn(field: StructField) = {
    fields :+= field
  }
}
