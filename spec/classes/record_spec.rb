require 'spec_helper'

module CassandraModel
  describe Record do
    let(:record_klass) do
      Class.new(Record) do
        extend DataModelling
        self.table_name = Faker::Lorem.word
        model_data do |inquirer, data_set|
          inquirer.knows_about(:key)
          data_set.knows_about(:value)
        end
      end
    end
    let(:table_name) { record_klass.table_name }
    let(:spark_context) { ConnectionCache[nil].spark_context }
    let(:keyspace) { ConnectionCache[nil].config[:keyspace] }
    let(:rdd_count) { rand(0..12345) }
    let(:rdd) { double(:rdd, count: rdd_count) }

    subject { record_klass }

    before do
      allow(Spark::Lib::SparkCassandraHelper).to receive(:cassandraTable).with(spark_context, keyspace, table_name).and_return(rdd)
    end

    describe '.rdd' do

      subject { record_klass.rdd }

      it { is_expected.to eq(rdd) }
    end

    its(:count) { is_expected.to eq(rdd_count) }
    its(:rdd_row_mapping) { is_expected.to be_nil }
    its(:sql_schema) { is_expected.to eq(Spark::SqlSchema.new(record_klass.cassandra_columns)) }

  end
end