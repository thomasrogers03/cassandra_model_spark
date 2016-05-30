require 'fileutils'

module CassandraModel
  module Spark
    class << self

      def root
        @gem_root ||= File.expand_path('../../..', __FILE__)
      end

      def config_path
        @config_path ||= "#{root}/config"
      end

      def config_file_path
        @config_file_path ||= "#{config_path}/spark.yml"
      end

      def config
        @config ||= if File.exists?(config_file_path)
                      YAML.load(File.read(config_file_path))
                    else
                      {}
                    end
      end

      def home
        @home ||= (ENV['SPARK_HOME'] || default_home)
      end

      def classpath
        @classpath ||= (ENV['SPARK_CLASSPATH'] || default_classpath)
      end

      @@shutdown = false

      def force_shutdown!
        @@shutdown = true
        Spark.application.java_spark_context.stop
      end

      @@application = Application.new(Spark.config)
      at_exit do
        if Spark.application.has_spark_context? && !@@shutdown
          Logging.logger.info 'Shutting down spark context'
          force_shutdown!
        end
      end

      def application
        @@application
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
