require 'jruby_spec_helper'

describe Kafka::Producer do

  let(:msg_klass) { Kafka::Producer::KeyedMessage }
  let(:messages) { [] }
  let(:java_producer) { double(:producer) }

  subject { Kafka::Producer.allocate }

  before { allow(subject).to receive(:producer).and_return(java_producer) }

  describe '#send_msgs' do
    let(:mapped_messages) { [] }
    let(:expected_messages) do
      java.util.ArrayList.new(mapped_messages)
    end

    it 'should call #send on the producer with an empty list' do
      expect(java_producer).to receive(:send).with(expected_messages)
      subject.send_msgs(messages)
    end

    context 'with messages' do
      let(:topic) { Faker::Lorem.sentence }
      let(:key) { Faker::Lorem.sentence }
      let(:message) { Faker::Lorem.sentence }
      let(:messages) { [[topic, key, message]] }
      let(:mapped_messages) do
        [msg_klass.new(topic, key, message)]
      end

      it 'should call #send on the producer with a single message' do
        expect(java_producer).to receive(:send).with(expected_messages)
        subject.send_msgs(messages)
      end

      context 'with multiple messages' do
        let(:topic_two) { Faker::Lorem.sentence }
        let(:key_two) { Faker::Lorem.sentence }
        let(:message_two) { Faker::Lorem.sentence }
        let(:messages) { [[topic, key, message], [topic_two, key_two, message_two]] }
        let(:mapped_messages) do
          [msg_klass.new(topic, key, message), msg_klass.new(topic_two, key_two, message_two)]
        end

        it 'should call #send on the producer with all message' do
          expect(java_producer).to receive(:send).with(expected_messages)
          subject.send_msgs(messages)
        end
      end
    end
  end
end