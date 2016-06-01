require 'spec_helper'

module CassandraModel
  module Spark
    describe KafkaReactor do

      let(:size) { 1 }
      let(:seed_brokers) { Faker::Internet.ip_v4_address }
      let(:options) { {seed_brokers: seed_brokers} }
      let(:producer) { double(:producer, produce: nil, deliver_messages: nil) }
      let(:kafka) { double(:kafka, producer: producer) }
      let(:pool) { double(:pool) }

      subject { KafkaReactor.new(size, options) }

      before do
        allow(Kafka).to receive(:new).with(seed_brokers: seed_brokers).and_return(kafka)
      end

      it { is_expected.to be_a_kind_of(::BatchReactor::ReactorCluster) }

      describe 'initialization' do
        let(:size) { rand(1..10) }
        let(:options) do
          {
              Faker::Lorem.word => Faker::Lorem.word,
              Faker::Lorem.sentence => Faker::Lorem.word
          }
        end

        it 'initializes the cluster with the specified size and options' do
          expect(::BatchReactor::Reactor).to receive(:new).with(options).exactly(size).times.and_call_original
          subject
        end

      end

      describe '#perform_within_batch' do
        let(:partition) { rand(0..1000) }
        let(:pool_block) { lambda { |block| block.call } }

        before do
          allow(ConnectionPool).to receive(:new).with(size: size) do |&block|
            allow(pool).to receive(:with).and_yield(pool_block[block])
            pool
          end
          subject.start.get
        end

        after { subject.stop.get }

        it 'should ensure that we do not use a partition larger than the number of reactors' do
          expect do
            subject.perform_within_batch(size + 1) do |producer|
              producer.produce
            end.get
          end.not_to raise_error
        end

        it 'should yield the producer from the created kafka connection' do
          expect(producer).to receive(:produce)
          subject.perform_within_batch(partition) do |producer|
            producer.produce
          end.get
        end

        it 'should deliver the messages on the producer' do
          expect(producer).to receive(:deliver_messages)
          subject.perform_within_batch(partition) do |producer|
            producer.produce
          end.get
        end

        it 'gracefully handles exceptions' do
          expect do
            subject.perform_within_batch(partition) do |_|
              raise 'It Broke!'
            end.get
          end.to raise_error('It Broke!')
        end

        describe 'thread safety' do
          let(:size) { rand(1..10) }
          let(:kafka_two) { double(:kafka) }
          let(:producer_two) { double(:producer, produce: nil, deliver_messages: nil) }
          let(:pool_block) do
            lambda do |_|
              allow(kafka_two).to receive(:producer).and_return(producer_two)
              kafka_two
            end
          end

          it 'uses a connection pool for producing' do
            result_producer = subject.perform_within_batch(partition) { |producer| producer }.get
            expect(result_producer).to eq(producer_two)
          end
        end
      end

    end
  end
end