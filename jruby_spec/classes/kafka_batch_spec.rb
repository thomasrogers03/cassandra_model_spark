require 'jruby_spec_helper'

module CassandraModel
  module Spark
    describe KafkaBatch do

      describe '#produce' do
        let(:message) { Faker::Lorem.sentence }
        let(:topic) { Faker::Lorem.sentence }
        let(:key) { Faker::Lorem.sentence }
        let(:options) { {key: key, topic: topic} }

        before { subject.produce(message, options) }

        its(:messages) { is_expected.to include([topic, key, message]) }

        context 'with multiple messages' do
          let(:message_two) { Faker::Lorem.sentence }
          let(:topic_two) { Faker::Lorem.sentence }
          let(:key_two) { Faker::Lorem.sentence }
          let(:options_two) { {key: key_two, topic: topic_two} }

          before { subject.produce(message_two, options_two) }

          its(:messages) { is_expected.to include([topic, key, message]) }
          its(:messages) { is_expected.to include([topic_two, key_two, message_two]) }

        end
      end

    end
  end
end