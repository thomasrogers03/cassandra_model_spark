require 'optparse'
require_relative 'spark'

options = {}
OptionParser.new do |opts|
  opts.banner = 'Usage: build.rb [--only-ext]'
  opts.on('-e', '--only-ext', 'Build only extension') do
    options[:only_ext] = true
  end
end.parse!

ASSEMBLY_PATH = '/ext/scala_helper'

Dir.chdir("#{CassandraModel::Spark.root}#{ASSEMBLY_PATH}") do
  puts '=> building extension...'
  cmd = 'sbt package'
  cmd << ' assemblyPackageDependency' unless options[:only_ext]
  system(ENV.to_hash.merge('TARGET_DIR' => CassandraModel::Spark.classpath), cmd)
  %w(bin sbin).each do |path|
    puts "=> copying #{path}/ to #{CassandraModel::Spark.home}/"
    `cp -R #{CassandraModel::Spark.root}#{ASSEMBLY_PATH}/#{path}/ #{CassandraModel::Spark.home}/`
  end
  `touch #{CassandraModel::Spark.home}/RELEASE`
end
