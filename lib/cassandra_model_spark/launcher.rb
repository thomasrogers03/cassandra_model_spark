require 'socket'

module CassandraModel
  module Spark
    class Launcher
      def start_master
        system(env, "#{spark_daemon} start #{start_master_args}")
        add_master_jars
      end

      def start_slaves
        workers.map do |worker|
          system(env, "#{spark_daemon} start #{start_slave_args(worker)}")
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

      def add_master_jars
        ConnectionCache[nil].tap do |connection|
          connection.config = {spark: {master: master_url}}
          connection.spark_context.addJar("#{Spark.classpath}/cmodel_scala_helper.jar")
        end
      end

      def workers
        slave_config[:worker_count].to_i.times.map { |index| index + 1 }
      end

      def start_master_args
        "#{master_args} --ip #{Socket.gethostname} --port #{master_config[:master_port]} --webui-port #{master_config[:ui_port]} -h #{master_config[:host]}"
      end

      def start_slave_args(id)
        "#{slave_args(id)} --webui-port #{slave_config[:ui_port]} #{master_url}"
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
          override_config = ConnectionCache[nil].config.fetch(:spark_daemon) { {} }
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
        @env ||= {
            'SPARK_HOME' => Spark.home,
            'SPARK_CLASSPATH' => Spark.classpath,
            'SPARK_JARS' => Dir["#{Spark.classpath}/*.jar"] * ',',
        }.merge(ENV.to_hash)
      end

    end
  end
end