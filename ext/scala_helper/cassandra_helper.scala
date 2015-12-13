package org.apache.spark.api.cassandra_model

import org.apache.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import java.util._
import scala.collection.JavaConversions.mapAsScalaMap

object CassandraHelper {
  def cassandraTable(sc: SparkContext, keyspace: String, table: String) = {
    sc.cassandraTable(keyspace, table)
  }
  def filterRDD(rdd: CassandraRDD[CassandraRow], restriction: HashMap[String, Any]) = {
    var result = rdd
    for ((k,v) <- restriction) result = result.where(k + " = ?", v)
    result
  }
}