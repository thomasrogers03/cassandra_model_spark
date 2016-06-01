require 'jruby_spec_helper'

module CassandraModel
  module Spark
    describe KafkaReactor do

      let(:size) { 1 }
      let(:seed_brokers) { Faker::Internet.ip_v4_address }
      let(:options) { {seed_brokers: seed_brokers} }
      let(:producer) { double(:producer, connect: nil, close: nil, send_msgs: nil) }
      let(:pool) { double(:pool) }
      let(:producer_options) { {broker_list: seed_brokers, 'serializer.class' => 'kafka.serializer.StringEncoder'} }
      let(:reactor) { KafkaReactor.new(size, options) }

      subject { reactor }

      before do
        allow(Kafka::Producer).to receive(:new).with(producer_options).and_return(producer)
      end

      it { is_expected.to be_a_kind_of(::BatchReactor::ReactorCluster) }

      describe 'initialization' do
        let(:size) { rand(1..10) }
        let(:options) do
          {
              seed_brokers: seed_brokers,
              Faker::Lorem.word => Faker::Lorem.word,
              Faker::Lorem.sentence => Faker::Lorem.word
          }
        end

        it 'initializes the cluster with the specified size and options' do
          expect(::BatchReactor::Reactor).to receive(:new).with(options.except(:seed_brokers)).exactly(size).times.and_call_original
          subject
        end

        it 'connects the producer' do
          expect(producer).to receive(:connect)
          subject
        end
      end

      describe '#stop' do
        let(:size) { rand(1..10) }
        let(:internal_reactors) { reactor.instance_variable_get(:@reactors) }

        before { reactor.start.get }
        subject { reactor.stop.get }

        it { is_expected.to eq(internal_reactors) }

        it 'stops all the reactors in the cluster' do
          internal_reactors.each { |reactor| expect(reactor).to receive(:stop).and_call_original }
          subject
        end

        it 'disconnects from kafka' do
          expect(producer).to receive(:close)
          subject
        end
      end

      describe '#perform_within_batch' do
        let(:partition) { rand(0..1000) }
        let(:pool_block) { lambda { |block| block.call } }
        let(:topic) { Faker::Lorem.sentence }
        let(:key) { Faker::Lorem.sentence }
        let(:message) { Faker::Lorem.sentence }
        let(:expected_message) { [topic, key, message] }

        before { subject.start.get }

        after { subject.stop.get }

        it 'should ensure that we do not use a partition larger than the number of reactors' do
          expect do
            subject.perform_within_batch(size + 1) do |producer|
              producer.produce(message, topic: topic, key: key)
            end.get
          end.not_to raise_error
        end

        it 'should yield the producer from the created kafka connection' do
          expect(producer).to receive(:send_msgs).with([expected_message])
          subject.perform_within_batch(partition) do |producer|
            producer.produce(message, topic: topic, key: key)
          end.get
        end

        it 'gracefully handles exceptions' do
          expect do
            subject.perform_within_batch(partition) do |_|
              raise 'It Broke!'
            end.get
          end.to raise_error('It Broke!')
        end
      end

    end
  end
end