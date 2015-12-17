require 'rspec'

module CassandraModel
  describe QueryBuilder do
    let(:rdd) { double(:rdd) }
    let(:table_name) { Faker::Lorem.word }
    let(:record_klass) { double(:klass, rdd: rdd, table_name: table_name) }
    let(:restriction_key) { Faker::Lorem.word.to_sym }
    let(:restriction_value) { Faker::Lorem.word }
    let(:restriction) { {restriction_key => restriction_value} }
    let(:updated_restriction) do
      restriction.inject({}) do |memo, (key, value)|
        updated_key = if value.is_a?(Array)
                        updated_key = key.is_a?(ThomasUtils::KeyComparer) ? key.to_s : "#{key} IN"
                        "#{updated_key} (#{(%w(?)*value.count) * ', '})"
                      else
                        key.is_a?(ThomasUtils::KeyComparer) ? "#{key} ?" : "#{key} = ?"
                      end
        memo.merge!(updated_key => value)
      end
    end
    let(:java_restriction) { JavaHash[updated_restriction] }
    let(:query_builder) { QueryBuilder.new(record_klass).where(restriction) }

    before do
      allow(record_klass).to receive(:restriction_attributes) do |restriction|
        restriction
      end
    end

    describe '#group' do
      it 'should pass group options to the underlying Record query' do
        query_builder.group(:make)
        expect(record_klass).to receive(:request).with(a_kind_of(Hash), group: [:make])
        query_builder.get
      end

      it 'should return the QueryBuilder' do
        expect(query_builder.group(:make)).to eq(query_builder)
      end

      context 'with multiple columns' do
        it 'should pass group options to the underlying Record query' do
          query_builder.group(:make, :model)
          expect(record_klass).to receive(:request).with(a_kind_of(Hash), group: [:make, :model])
          query_builder.get
        end
      end

      context 'when called multiple times' do
        it 'should chain the values' do
          query_builder.group(:make)
          query_builder.group(:model)
          expect(record_klass).to receive(:request).with(a_kind_of(Hash), group: [:make, :model])
          query_builder.get
        end
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

        context 'when a restriction key is a ThomasUtils::KeyComparer' do
          let(:restriction_key) { :clustering.gt }

          its(:restriction) { is_expected.to eq(java_restriction) }
        end

        context 'when the restriction contains an array value' do
          let(:restriction_value) { [Faker::Lorem.word] }

          its(:restriction) { is_expected.to eq(java_restriction) }

          context 'when a restriction key is a ThomasUtils::KeyComparer' do
            let(:restriction_key) { :clustering.gt }

            its(:restriction) { is_expected.to eq(java_restriction) }
          end
        end

        context 'when the record klass modifies the restriction for querying' do
          let(:java_restriction) { JavaHash["rk_#{restriction_key} = ?" => restriction_value] }

          before do
            allow(record_klass).to receive(:restriction_attributes) do |restriction|
              restriction.inject({}) { |memo, (key, value)| memo.merge!(:"rk_#{key}" => value) }
            end
          end

          its(:restriction) { is_expected.to eq(java_restriction) }
        end
      end

      describe 'DataFrame options' do
        let(:data_frame) { query_builder.as_data_frame(sql_context: sql_context, alias: table_alias) }
        let(:sql_context) { double(:sql_context) }
        let(:table_alias) { Faker::Lorem.word }

        its(:sql_context) { is_expected.to eq(sql_context) }
        its(:table_name) { is_expected.to eq(table_alias) }
      end
    end
  end
end
