require 'rspec'

module CassandraModel
  describe QueryBuilder do
    let(:rdd) { double(:rdd) }
    let(:record_klass) { double(:klass, rdd: rdd) }
    let(:restriction_key) { Faker::Lorem.word.to_sym }
    let(:restriction_value) { Faker::Lorem.word }
    let(:restriction) { {restriction_key => restriction_value} }
    let(:java_restriction) { JavaHash[restriction.stringify_keys] }
    let(:query_builder) { QueryBuilder.new(record_klass).where(restriction) }

    before do
      allow(record_klass).to receive(:restriction_attributes) do |restriction|
        restriction
      end
    end

    describe '#as_data_frame' do
      let(:data_frame) { query_builder.as_data_frame }
      let(:frame_klass) { data_frame.send(:record_klass) }
      let(:frame_rdd) { data_frame.send(:rdd) }

      subject { data_frame }

      it { is_expected.to be_a_kind_of(Spark::DataFrame) }

      it { expect(frame_klass).to eq(record_klass) }

      describe 'filtering the rdd' do
        subject { frame_rdd }

        its(:rdd) { is_expected.to eq(rdd) }
        its(:restriction) { is_expected.to eq(java_restriction) }

        context 'when the record klass modifies the restriction for querying' do
          let(:java_restriction) { JavaHash["rk_#{restriction_key}" => restriction_value] }

          before do
            allow(record_klass).to receive(:restriction_attributes) do |restriction|
              restriction.inject({}) { |memo, (key, value)| memo.merge!(:"rk_#{key}" => value) }
            end
          end

          its(:restriction) { is_expected.to eq(java_restriction) }
        end
      end
    end
  end
end