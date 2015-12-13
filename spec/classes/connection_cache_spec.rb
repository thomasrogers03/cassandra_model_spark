require 'rspec'

module CassandraModel
  describe ConnectionCache do
    class ConnectionCache
      def self.reset!
        @@cache.clear
      end
    end

    after { ConnectionCache.reset! }

    describe '.clear' do
      before do
        ConnectionCache[nil]
        ConnectionCache['counters'].config = { hosts: %w(athena) }
      end

      it 'should shutdown all active spark contexts' do
        expect(ConnectionCache[nil].java_spark_context).to receive(:stop)
        expect(ConnectionCache['counters'].java_spark_context).to receive(:stop)
        ConnectionCache.clear
      end

      it 'should shutdown all active connections' do
        expect(ConnectionCache[nil]).to receive(:shutdown)
        expect(ConnectionCache['counters']).to receive(:shutdown)
        ConnectionCache.clear
      end

      it 'should clear the connection cache' do
        prev_connection = ConnectionCache[nil]
        ConnectionCache.clear
        expect(ConnectionCache[nil]).not_to eq(prev_connection)
      end
    end
  end
end