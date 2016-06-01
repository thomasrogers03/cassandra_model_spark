#--
# Copyright 2015 Thomas RM Rogers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require 'yaml'
require 'logger'

require 'concurrent'
require 'cassandra'
require 'active_support/all'
require 'active_support/core_ext/class/attribute_accessors'
require 'thomas_utils'
require 'batch_reactor'
require 'cassandra_model'
require 'connection_pool'
if RUBY_PLATFORM == 'java'
  require 'jruby-kafka'
  require 'cassandra_model_spark/kafka_batch'
  require 'cassandra_model_spark/kafka_producer'
else
  require 'ruby-kafka'
  require 'rjb' unless CassandraModel.const_defined?('NO_BRIDGE')
  require 'cassandra_model_spark/kafka_reactor'
end
require 'cassandra_model_spark/application'
require 'cassandra_model_spark/spark'

unless CassandraModel.const_defined?('NO_BRIDGE')
  require 'cassandra_model_spark/java_bridge'
  Dir["#{CassandraModel::Spark.classpath}/*.jar"].each { |file| require file }
  initialize_java_engine
  require 'cassandra_model_spark/java_classes'
end

require 'cassandra_model_spark/record'
require 'cassandra_model_spark/query_builder'
require 'cassandra_model_spark/sql_schema'
require 'cassandra_model_spark/schema'
require 'cassandra_model_spark/data_frame'
require 'cassandra_model_spark/column_cast'
