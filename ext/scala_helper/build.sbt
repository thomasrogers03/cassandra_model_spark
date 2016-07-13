import AssemblyKeys._

assemblySettings

name := "cmodel_scala_helper"
version := "0.0.1"
scalaVersion := "2.10.4"

val sparkVersion = "1.6.1"
val hadoopClientVersion = "1.0.4"
val cassandraConnectionVersion = "1.6.0-M1"

val _targetDir = scala.util.Properties.envOrElse("TARGET_DIR", "target")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")
scalacOptions ++= Seq("-feature")

artifactPath in Compile in packageBin := file(s"${_targetDir}/cmodel_scala_helper.jar")
outputPath in packageDependency := file(s"${_targetDir}/spark-assembly-${sparkVersion}-cassandra_model-hadoop${hadoopClientVersion}.jar")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion excludeAll(ExclusionRule(organization = "org.apache.hadoop")),
  "org.apache.spark" % "spark-repl_2.10" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-flume" % sparkVersion,
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.hadoop" %  "hadoop-client" % hadoopClientVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % cassandraConnectionVersion,
  "com.datastax.spark" %% "spark-cassandra-connector-java" % cassandraConnectionVersion,
  "com.github.fommil.netlib" % "all" % "1.1.2",
  "com.databricks" % "spark-csv_2.10" % "1.3.0",
  "org.luaj" % "luaj-jse" % "3.0.1",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
)

resolvers ++= Seq(
  "JBoss Repository"     at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository"     at "http://repo.spray.cc/",
  "Cloudera Repository"  at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository"      at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase"         at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo"   at "http://maven.twttr.com/",
  "scala-tools"          at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository"  at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}
