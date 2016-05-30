require 'socket'

module CassandraModel
  module Spark
    class Launcher
      def start_master
        system(env, "#{spark_daemon} start #{start_master_args}")
        add_master_jars
      end

      def run_master
        validate_env!

        result = Spark::Lib::SparkMaster.startRpcEnvAndEndpoint(master_config[:host], master_config[:master_port], master_config[:ui_port], spark_conf)._1
        wait_for_shutdown do
          result.shutdown
          result.awaitTermination
        end
      end

      def start_slaves
        workers.map do |worker|
          system(env, "#{spark_daemon} start #{start_slave_args(worker)}")
        end
      end

      def run_slave
        validate_env!

        result = Spark::Lib::SparkWorkerStarter.startWorker(master_url, slave_config[:host], master_config[:master_port], master_config[:ui_port], spark_conf)
        wait_for_shutdown do
          result.shutdown
          result.awaitTermination
        end
      end

      def stop_master
        system(env, "#{spark_daemon} stop #{master_args}")
      end

      def stop_slaves
        workers.map do |worker|
          system(env, "#{spark_daemon} stop #{slave_args(worker)}")
        end
      end

      private

      def spark_conf
        @spark_conf ||= Spark.application.send(:spark_conf)
      end

      def wait_for_shutdown
        begin
          loop { sleep 0.2 }
        rescue Interrupt
          yield
        end
      end

      def to_argv(args)
        args.split.to_java_argv
      end

      def validate_env!
        unless ENV['SPARK_HOME'] && File.expand_path(ENV['SPARK_HOME']) == Spark.home
          raise 'Spark environment not set correctly'
        end
      end

      def add_master_jars
        Spark.application do |connection|
          connection.spark_config = {spark: {master: master_url}}
          connection.spark_context.addJar("#{Spark.classpath}/cmodel_scala_helper.jar")
        end
        Spark.force_shutdown!
      end

      def workers
        slave_config[:worker_count].to_i.times.map { |index| index + 1 }
      end

      def start_master_args
        "#{master_args} #{run_master_args}"
      end

      def run_master_args
        "--ip #{Socket.gethostname} --port #{master_config[:master_port]} --webui-port #{master_config[:ui_port]} -h #{master_config[:host]}"
      end

      def start_slave_args(id)
        "#{slave_args(id)} #{run_slave_args}"
      end

      def run_slave_args
        "--webui-port #{slave_config[:ui_port]} #{master_url}"
      end

      def master_args
        "org.apache.spark.deploy.master.Master #{master_config[:id]}"
      end

      def slave_args(id)
        "org.apache.spark.deploy.worker.Worker #{id}"
      end

      def spark_daemon
        "#{Spark.home}/sbin/spark-daemon.sh"
      end

      def master_url
        "spark://#{master_config[:host]}:#{master_config[:master_port]}"
      end

      def master_config
        config.merge(config.fetch(:master) { {} })
      end

      def slave_config
        config.merge(config.fetch(:slave) { {} })
      end

      def config
        @config ||= begin
          override_config = Spark.application.spark_config.fetch(:spark_daemon) { {} }
          {
              id: 1,
              ui_port: 8180,
              master_port: 7077,
              worker_count: 1,
              host: Socket.gethostname,
          }.merge(override_config)
        end
      end

      def env
        @env ||= spark_env.merge(ENV.to_hash)
      end

      def spark_env
        @spark_env ||= {
            'SPARK_HOME' => Spark.home,
            'SPARK_CLASSPATH' => Spark.classpath,
            'SPARK_JARS' => Dir["#{Spark.classpath}/*.jar"] * ',',
        }
      end

    end
  end
end
