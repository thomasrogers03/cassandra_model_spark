#!/usr/bin/env ruby

lib = File.expand_path(File.dirname(__FILE__) + '/../lib')
$LOAD_PATH.unshift(lib) if File.directory?(lib) && !$LOAD_PATH.include?(lib)

require 'bundler/setup'
require 'cassandra_model_spark/spark'

print "export SPARK_HOME=#{CassandraModel::Spark.home} SPARK_CLASSPATH=#{CassandraModel::Spark.classpath}"


