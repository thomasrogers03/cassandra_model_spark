require 'spec_helper'

module CassandraModel
  module Spark
    describe Application do
      let(:config) { {hosts: 3.times.map { Faker::Internet.ip_v4_address }} }
      let(:connection) { Application.new(config[:spark]) }

      describe '#config' do
        subject { connection }

        its(:config) { is_expected.to eq(config[:spark]) }

        context 'when overriding the configuration after initialization' do
          let(:override_config) { Faker::Lorem.words }

          before { subject.config = override_config }
          its(:config) { is_expected.to eq(override_config) }
        end
      end

      describe '#java_spark_context' do
        let(:default_config) do
          Spark::Lib::SparkConf.from_hash({
                                              'spark.app.name' => 'cassandra_model_spark',
                                              'spark.master' => 'local[*]',
                                              'spark.cassandra.connection.host' => 'localhost',
                                          })
        end

        subject { connection.java_spark_context }

        it { is_expected.to be_a_kind_of(Spark::Lib::JavaSparkContext) }

        context 'when creating the spark context raises an error' do
          before { allow(Spark::Lib::JavaSparkContext).to receive(:new).and_raise('Not enough heap!') }

          it 'should re-raise the error' do
            expect { subject }.to raise_error('Not enough heap!')
          end
        end

        it 'should not initialize the spark context multiple times' do
          connection.java_spark_context
          expect(Spark::Lib::JavaSparkContext).not_to receive(:new)
          connection.java_spark_context
        end

        it 'should add the helper jar' do
          connection.java_spark_context
          expect(subject.sc.jars).to include("#{Spark.classpath}/cmodel_scala_helper.jar")
        end

        its(:config) { is_expected.to eq(default_config) }

        context 'with an overriding spark config' do
          let(:config) do
            {
                hosts: %w(cassandra.local),
                spark: {
                    app: {name: 'sparky'},
                    master: 'spark.dev',
                    executor: {memory: '512g'},
                }
            }
          end
          let(:spark_config) do
            Spark::Lib::SparkConf.from_hash({
                                                'spark.app.name' => 'sparky',
                                                'spark.master' => 'spark.dev',
                                                'spark.executor.memory' => '512g',
                                                'spark.cassandra.connection.host' => 'localhost',
                                            })
          end

          its(:config) { is_expected.to eq(spark_config) }
        end
      end

      describe '#spark_context' do
        let!(:context) { connection.java_spark_context }

        subject { connection.spark_context }

        it { is_expected.to eq(context.sc) }
      end

      describe '#has_spark_context?' do
        subject { connection.has_spark_context? }

        it { is_expected.to eq(false) }

        context 'when the context has been created' do
          before { connection.java_spark_context }

          it { is_expected.to eq(true) }
        end
      end

      describe '#create_java_spark_streaming_context' do
        let!(:context) { connection.java_spark_context }

        subject { connection.create_java_spark_streaming_context }

        it { is_expected.to be_a_kind_of(Spark::Lib::JavaSparkStreamingContext) }
        its(:sparkContext) { is_expected.to eq(context) }
        its(:duration) { is_expected.to eq(Spark::Lib::SparkDuration.new(2000)) }
      end

    end
  end
end
