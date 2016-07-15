require 'fileutils'

module CassandraModel
  module Spark
    class << self

      def root
        @gem_root ||= File.expand_path('../../..', __FILE__)
      end

      def application_root
        @app_root ||= File.expand_path('./')
      end

      def home
        @home ||= (ENV['SPARK_HOME'] || default_home)
      end

      def config_path
        @config_path ||= if File.exists?(spark_config_path(application_root))
                           "#{application_root}/config"
                         else
                           "#{home}/config"
                         end
      end

      def config_file_path
        @config_file_path ||= spark_config_path(config_path)
      end

      def config
        @config ||= if File.exists?(config_file_path)
                      config = YAML.load(File.read(config_file_path))
                      config = config[ENV['RAILS_ENV']] if ENV['RAILS_ENV']
                      config
                    else
                      {}
                    end
      end

      def classpath
        @classpath ||= (ENV['SPARK_CLASSPATH'] || default_classpath)
      end

      @@shutdown = false

      def force_shutdown!
        @@shutdown = true
        Spark.application.java_spark_context.stop
      end

      def application
        @@application
      end

      private

      def spark_config_path(directory)
        "#{directory}/config/spark.yml"
      end

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

      @@application = Application.new(Spark.config)
      at_exit do
        if Spark.application.has_spark_context? && !@@shutdown
          Logging.logger.info 'Shutting down spark context'
          @@shutdown = true
          Spark.application.java_spark_context.stop
        end
      end
    end
  end
end
