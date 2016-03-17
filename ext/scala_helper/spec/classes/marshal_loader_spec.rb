require 'scala_spec_helper'

describe ScalaMarshalLoader do
  let(:value) { nil }
  let(:header_magic) { 'MRSH' }
  let(:dump) { header_magic + Marshal.dump(value) }
  let(:loader) { ScalaMarshalLoader.new(dump) }

  describe '#getValue' do
    subject { loader.getValue }

    it { is_expected.to eq(nil) }

    context 'with an invalid header' do
      let(:header_magic) { 'MARSH' }

      it { expect { subject }.to raise_error }
    end

    describe 'numerics' do
      subject { loader.getValue.toString }

      context 'with an integer' do
        let(:value) { 0 }
        it { is_expected.to eq('0') }

        context 'when > 0 but <= 6' do
          let(:value) { 6 }
          it { is_expected.to eq('6') }
        end

        context 'when > 6' do
          let(:value) { 23452345 }
          it { is_expected.to eq('23452345') }
        end

        context 'when < 0' do
          let(:value) { -23452345 }
          it { is_expected.to eq('-23452345') }
        end
      end

      context 'with a double' do
        let(:value) { rand * 99999.0 }
        it { is_expected.to eq(value.to_s) }
      end
    end

    describe 'strings' do
      let(:encoding) { 'UTF-8' }
      let(:value) { Faker::Lorem.word.encode(encoding) }

      subject { loader.getValue.toString }

      it { is_expected.to eq(value) }

      context 'with an US-ASCII string' do
        let(:encoding) { 'US-ASCII' }
        it { is_expected.to eq(value) }
      end

      context 'with an BINARY string' do
        let(:encoding) { 'BINARY' }
        it { is_expected.to eq(value) }
      end
    end

    context 'with an array' do
      let(:value) { [] }
      it { is_expected.to eq([]) }
    end

    context 'with a hash' do
      let(:value) { {} }
      subject { Hash[loader.getValue.toSeq.array.map! { |pair| [pair._1.toString, pair._2.toString] if pair }.compact] }
      it { is_expected.to eq({}) }

      context 'having content' do
        let(:value) { {hello: 'world', 5 => 79.5} }
        it { is_expected.to eq('hello' => 'world', '5' => '79.5') }
      end
    end

    context 'with a symbol' do
      let(:symbol) { Faker::Lorem.word.to_sym }
      let(:value) { symbol }
      subject { loader.getValue.toString }

      it { is_expected.to eq(symbol.to_s) }

      context 'when the symbol appears multiple times' do
        let(:value) { [symbol, symbol] }
        subject { loader.getValue.map(&:toString) }

        it { is_expected.to eq(value.map(&:to_s)) }
      end
    end

  end
end