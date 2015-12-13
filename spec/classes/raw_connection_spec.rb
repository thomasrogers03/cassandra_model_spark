require 'rspec'

module CassandraModel
  describe RawConnection do
    let(:config) { {hosts: 3.times.map { Faker::Internet.ip_v4_address }} }
    let(:connection) { RawConnection.new.tap { |conn| conn.config = config } }

    describe '#java_spark_context' do
      let(:configured_host) { connection.config[:hosts].first }
      let(:default_config) do
        SparkConf.from_hash({
                                'spark.app.name' => 'cassandra_model_spark',
                                'spark.master' => 'local[*]',
                                'spark.cassandra.connection.host' => configured_host,
                            })
      end

      subject { connection.java_spark_context }

      its(:config) { is_expected.to eq(default_config) }

      it 'should not initialize the spark context multiple times' do
        connection.java_spark_context
        expect(JavaSparkContext).not_to receive(:new)
        connection.java_spark_context
      end
    end

    describe '#spark_context' do
      let!(:context) { connection.java_spark_context }

      subject { connection.spark_context }

      it { is_expected.to eq(context.sc) }
    end

  end
end