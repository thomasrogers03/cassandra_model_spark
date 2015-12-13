require 'rspec'

module CassandraModel
  describe Record do
    class Record
      def self.reset!
        @spark_rdd = nil
      end
    end

    let(:table_name) { Faker::Lorem.word }
    let(:spark_context) { ConnectionCache[nil].spark_context }
    let(:keyspace) { ConnectionCache[nil].config[:keyspace] }
    let(:rdd_count) { rand(0..12345) }
    let(:rdd) { double(:rdd, count: rdd_count) }

    subject { Record }

    before do
      allow(Record).to receive(:table_name).and_return(table_name)
      allow(SparkCassandraHelper).to receive(:cassandra_table).with(spark_context, keyspace, table_name).and_return(rdd)
    end

    after { Record.reset! }

    describe '.rdd' do

      subject { Record.rdd }

      it { is_expected.to eq(rdd) }
    end

    its(:count) { is_expected.to eq(rdd_count) }

  end
end