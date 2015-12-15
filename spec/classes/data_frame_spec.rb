require 'rspec'

module CassandraModel
  module Spark
    describe DataFrame do
      let(:cassandra_columns) { {partition: :text} }
      let(:table_name) { Faker::Lorem.word }
      let(:table) { TableRedux.new(table_name) }
      let(:record_klass) do
        double(:klass, table: table, cassandra_columns: cassandra_columns, table_name: table_name)
      end
      let(:rdd) { double(:rdd) }
      let(:data_frame) { DataFrame.new(record_klass, rdd) }


      describe '#table_name' do
        subject { data_frame.table_name }

        it { is_expected.to eq(table_name) }

        context 'with a specific table name' do
          let(:alias_table_name) { Faker::Lorem.word }
          let(:data_frame) { DataFrame.new(record_klass, rdd, alias: alias_table_name) }

          it { is_expected.to eq(alias_table_name) }
        end
      end

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

        context 'when the sql context is specified through the initializer' do
          let(:sql_context) { double(:sql_context) }
          let(:data_frame) { DataFrame.new(record_klass, rdd, sql_context: sql_context) }

          it { is_expected.to eq(sql_context) }
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

        it 'should register a temp table with the name of the table associated with the frame' do
          expect_any_instance_of(SqlDataFrame).to receive(:register_temp_table).with(table_name)
          data_frame.spark_data_frame
        end

        context 'with a specific table name specified' do
          let(:alias_table_name) { Faker::Lorem.word }
          let(:data_frame) { DataFrame.new(record_klass, rdd, alias: alias_table_name) }

          it 'should register a temp table with the alias' do
            expect_any_instance_of(SqlDataFrame).to receive(:register_temp_table).with(alias_table_name)
            data_frame.spark_data_frame
          end
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

        it 'should yield within context of the frame' do
          frame = nil
          data_frame.cached { frame = self }
          expect(frame).to eq(data_frame)
        end

        it 'should cache the data frame' do
          cached = nil
          data_frame.cached { cached = spark_data_frame.cached? }
          expect(cached).to eq(true)
        end

        it 'should uncache it afterwards' do
          data_frame.cached {}
          expect(data_frame.spark_data_frame).not_to be_cached
        end
      end

      describe 'querying the data frame' do
        subject { data_frame }

        it_behaves_like 'a query helper'
      end

      shared_examples_for 'an async method not yet implemented' do |method|
        describe "##{method}" do
          subject { data_frame.public_send(method, {}, {}) }

          it { expect { subject.get }.to raise_error(NotImplementedError) }
        end
      end

      describe '#request_async' do
        subject { data_frame.request_async({}) }
        it { is_expected.to be_a_kind_of(ResultPaginator) }
      end

      it_behaves_like 'an async method not yet implemented', :request_async
      it_behaves_like 'an async method not yet implemented', :first_async

      describe '#query' do
        let(:sql_context) { double(:sql_context) }
        let(:query) { double(:query) }
        let(:restriction) { {} }
        let(:options) { {} }
        let(:query_sql) { "SELECT * FROM #{table_name}" }
        let(:data_frame) { DataFrame.new(record_klass, rdd, sql_context: sql_context) }

        subject { data_frame.query(restriction, options) }

        before do
          allow(sql_context).to receive(:sql).with(query_sql).and_return(query)
          allow(record_klass).to(receive(:select_columns)) { |columns| columns }
        end

        it { is_expected.to eq(query) }

        describe 'restricting the data set' do
          let(:restriction) { { partition: 47 } }
          let(:query_sql) { "SELECT * FROM #{table_name} WHERE partition = 47" }

          it { is_expected.to eq(query) }

          context 'with a multi-column restriction' do
            let(:restriction) { { partition: 30, clustering: 20.0 } }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE partition = 30 AND clustering = 20.0" }

            it { is_expected.to eq(query) }
          end

          context 'when the restriction contains strings' do
            let(:restriction) { { partition: 'part' } }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE partition = 'part'" }

            it { is_expected.to eq(query) }
          end

          context 'when the restriction contains a timestamp' do
            let(:time) { Time.now }
            let(:restriction) { { partition: time } }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE partition = '#{time}'" }

            it { is_expected.to eq(query) }
          end
        end

        context 'with a different columns selected' do
          let(:options) { {select: [:partition]} }
          let(:query_sql) { "SELECT partition FROM #{table_name}" }

          it { is_expected.to eq(query) }

          context 'with multiple columns' do
            let(:options) { {select: [:partition, :clustering]} }
            let(:query_sql) { "SELECT partition, clustering FROM #{table_name}" }

            it { is_expected.to eq(query) }
          end

          context 'when the columns are mapped' do
            let(:query_sql) { "SELECT rk_partition FROM #{table_name}" }

            before do
              allow(record_klass).to(receive(:select_columns)) do |columns|
                columns.map { |key| :"rk_#{key}" }
              end
            end

            it { is_expected.to eq(query) }
          end
        end
      end

    end
  end
end
