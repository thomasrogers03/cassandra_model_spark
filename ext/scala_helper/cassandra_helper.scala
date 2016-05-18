package org.apache.spark.api.cassandra_model

import org.apache.spark._
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.cql._
import java.util._
import scala.collection.JavaConversions.mapAsScalaMap

object CassandraHelper {
  def cassandraTable(sc: SparkContext, keyspace: String, table: String) = {
    sc.cassandraTable(keyspace, table)
  }

  def saveRDDToCassandra(sc: SparkContext, rdd: CassandraRDD[CassandraRow], keyspace: String, table: String, host: String) = {
    implicit val c = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", host))

    rdd.saveToCassandra(keyspace, table)
  }

  def filterRDD(rdd: CassandraRDD[CassandraRow], restriction: HashMap[String, Any]) = {
    var result = rdd
    for ((k, v) <- restriction) {
      result = v match {
        case (a: Array[Any]) => result.where(k, a: _*)
        case _ => result.where(k, v)
      }
    }
    result
  }
}
