require 'rspec'

module CassandraModel
  module Spark
    describe ColumnCast do
      let(:key) { Faker::Lorem.word.to_sym }
      let(:type) { %w(int double string timestamp).sample.to_sym }
      let(:upcase_type) { type.to_s.upcase }
      let(:cast) { ColumnCast.new(key, type) }
      let(:quote) { %w(` ' ").sample }
      let(:quoted_string) { "CAST(#{quote}#{key}#{quote} AS #{upcase_type})" }

      describe '#quote' do
        subject { cast.quote(quote) }

        it { is_expected.to eq(quoted_string) }

        context 'when the key responds to #quote' do
          let(:key) { double(:quoting_key, to_s: 'bananas') }
          let(:quoted_string) { "CAST(#{quote}bananas#{quote}.#{quote}seeds#{quote} AS #{upcase_type})" }

          before do
            allow(key).to receive(:quote) do |quote|
              "#{quote}bananas#{quote}.#{quote}seeds#{quote}"
            end
          end

          it { is_expected.to eq(quoted_string) }
        end
      end
      
      describe 'Symbol methods' do
        subject { key.cast_as(type).quote(quote) }

        it { is_expected.to eq(quoted_string) }
      end
    end
  end
end
