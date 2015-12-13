module CassandraModel
  NO_BRIDGE = true
end

require 'optparse'
require 'bundler'
Bundler.require :default

options = {}
OptionParser.new do |opts|
  opts.banner = 'Usage: build.rb [--only-ext]'
  opts.on('-e', '--only-ext', 'Build only extension') do
    options[:only_ext] = true
  end
end.parse!

Dir.chdir("#{CassandraModel::Spark.root}/ext/scala_helper") do
  puts '=> building extension...'
  cmd = 'sbt package'
  cmd << ' assemblyPackageDependency' unless options[:only_ext]
  system(cmd)
end
