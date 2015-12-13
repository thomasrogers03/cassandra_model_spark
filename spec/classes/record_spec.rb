require 'rspec'

module CassandraModel
  describe Record do
    class Record
      def self.reset!
        @spark_rdd = nil
      end
    end

    let(:table_name) { Faker::Lorem.word }

    before do
      allow(Record).to receive(:table_name).and_return(table_name)
    end

    after { Record.reset! }

    describe '.rdd' do
      let(:spark_context) { ConnectionCache[nil].spark_context }
      let(:keyspace) { ConnectionCache[nil].config[:keyspace] }
      let(:rdd) { SecureRandom.uuid }

      subject { Record.rdd }

      before do
        allow(SparkCassandraHelper).to receive(:cassandra_table).with(spark_context, keyspace, table_name).and_return(rdd)
      end

      it { is_expected.to eq(rdd) }
    end

  end
end