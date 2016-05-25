package org.apache.spark.api.cassandra_model

import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.cql._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import java.util._
import scala.collection.JavaConversions.mapAsScalaMap

object CassandraHelper {
  def cassandraTable(sc: SparkContext, keyspace: String, table: String) = {
    sc.cassandraTable(keyspace, table)
  }

  def cassandraTableForHost(sc: SparkContext, keyspace: String, table: String, host: String) = {
    implicit val c = CassandraConnector(sc.getConf.set("spark.cassandra.connection.host", host))

    sc.cassandraTable(keyspace, table)
  }

  def createKafkaDStream(ssc: StreamingContext, zookeeper: String, group: String, topics: Array[String]) = {
    val topic_map = topics.map((_, 1))
    KafkaUtils.createStream(ssc, zookeeper, group, topic_map.toMap)
  }

  def saveRDDToCassandra(sc: SparkContext, rdd: RDD[CassandraRow], keyspace: String, table: String, host: String) = {
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
