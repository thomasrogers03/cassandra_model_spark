require 'rspec'

module CassandraModel
  module Spark
    describe DataFrame do
      let(:cassandra_columns) { {partition: :text} }
      let(:record_klass) { Record }
      let(:rdd) { double(:rdd) }
      let(:data_frame) { DataFrame.new(record_klass, rdd) }

      before { allow(Record).to receive(:cassandra_columns).and_return(cassandra_columns) }

      describe '#sql_context' do
        let(:spark_context) { record_klass.table.connection.spark_context }
        let(:keyspace) { Faker::Lorem.word }
        subject { data_frame.sql_context }
        before { record_klass.table.connection.config = {keyspace: keyspace} }

        it { is_expected.to eq(CassandraSQLContext.new(spark_context)) }

        it 'should cache the value' do
          data_frame.sql_context
          expect(CassandraSQLContext).not_to receive(:new)
          data_frame.sql_context
        end

        it 'should set the keyspace from the record class' do
          expect(subject.keyspace).to eq(keyspace)
        end
      end

      describe '#spark_data_frame' do
        let(:sql_columns) { {'partition' => SqlStringType} }
        subject { data_frame.spark_data_frame }

        it { is_expected.to be_a_kind_of(SqlDataFrame) }

        it 'should instance-cache the frame' do
          data_frame.spark_data_frame
          expect(SqlDataFrame).not_to receive(:new)
          data_frame.spark_data_frame
        end

        its(:schema) { is_expected.to eq(sql_columns) }

        context 'with a different set of columns' do
          let(:cassandra_columns) { {partition: :text, clustering: :int} }
          let(:sql_columns) { {'partition' => SqlStringType, 'clustering' => SqlIntegerType} }

          its(:schema) { is_expected.to eq(sql_columns) }
        end

        shared_examples_for 'mapping a cassandra column type to a spark sql type' do |cassandra_type, sql_type|
          let(:cassandra_columns) { {partition: cassandra_type} }
          let(:sql_columns) { {'partition' => sql_type} }

          its(:schema) { is_expected.to eq(sql_columns) }
        end

        it_behaves_like 'mapping a cassandra column type to a spark sql type', :double, SqlDoubleType
        it_behaves_like 'mapping a cassandra column type to a spark sql type', :timestamp, SqlTimestampType
      end

      describe '#cached' do
        it 'should yield' do
          expect { |block| data_frame.cached(&block) }.to yield_control
        end

        it 'should cache the data frame' do
          data_frame.cached { expect(data_frame.spark_data_frame).to be_cached }
        end

        it 'should uncache it afterwards' do
          data_frame.cached { }
          expect(data_frame.spark_data_frame).not_to be_cached
        end
      end

    end
  end
end
