require 'fileutils'

module CassandraModel
  module Spark
    class << self
      def root
        @gem_root ||= File.expand_path('../../..', __FILE__)
      end

      def home
        @home ||= (ENV['SPARK_HOME'] || default_home)
      end

      def classpath
        @classpath ||= (ENV['SPARK_CLASSPATH'] || default_classpath)
      end

      private

      def default_classpath
        File.expand_path('./lib/', home).tap do |path|
          FileUtils.mkdir_p(path)
        end
      end

      def default_home
        File.expand_path('~/.cassandra_model_spark').tap do |path|
          FileUtils.mkdir_p(path)
        end
      end
    end
  end
end
