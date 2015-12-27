#!/usr/bin/env ruby

lib = File.expand_path(File.dirname(__FILE__) + '/../lib')
$LOAD_PATH.unshift(lib) if File.directory?(lib) && !$LOAD_PATH.include?(lib)

require 'bundler/setup'
require 'cassandra_model_spark'
require 'cassandra_model_spark/launcher'

command = ARGV.shift.downcase.to_sym
launcher = CassandraModel::Spark::Launcher.new
case command
  when :start
    launcher.start_master
  when :stop
    launcher.stop_master
  else
    puts '=> only supports start or stop'
end

