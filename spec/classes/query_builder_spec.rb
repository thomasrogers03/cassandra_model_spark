require 'rspec'

module CassandraModel
  describe QueryBuilder do
    let(:rdd) { double(:rdd) }
    let(:record_klass) { double(:klass, rdd: rdd) }
    let(:query_builder) { QueryBuilder.new(record_klass) }

    describe '#as_data_frame' do
      let(:data_frame) { query_builder.as_data_frame }
      let(:frame_klass) { data_frame.send(:record_klass) }
      let(:frame_rdd) { data_frame.send(:rdd) }

      subject { data_frame }

      it { is_expected.to be_a_kind_of(Spark::DataFrame) }

      it { expect(frame_klass).to eq(record_klass) }

      describe 'filtering the rdd' do
        subject { frame_rdd }


      end
    end
  end
end
