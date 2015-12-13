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
    end

  end
end