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

require 'concurrent'
require 'cassandra'
require 'active_support/all'
require 'active_support/core_ext/class/attribute_accessors'
require 'thomas_utils'
require 'batch_reactor'
require 'cassandra_model'
require 'rjb' unless RUBY_ENGINE == 'jruby' || CassandraModel.const_defined?('NO_BRIDGE')

module CassandraModel
  module Spark
    def self.root
      @gem_root ||= File.expand_path('../..', __FILE__)
    end
  end
end

unless CassandraModel.const_defined?('NO_BRIDGE')
  require 'cassandra_model_spark/java_bridge'
  Dir["#{CassandraModel::Spark.root}/ext/scala_helper/target/*.jar"].each { |file| require file }
  initialize_java_engine
  require 'cassandra_model_spark/java_classes'
end

require 'cassandra_model_spark/raw_connection'
require 'cassandra_model_spark/record'
