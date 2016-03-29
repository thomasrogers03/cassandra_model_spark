require 'integration_spec_helper'

describe 'querying a DataFrame' do
  let(:record_klass) { generate_composite_model(:frame_test, {key: :text}, {value: :text}, {description: :text}) }
  let!(:record) do
    record_klass.create(
        key: Faker::Lorem.word,
        value: Faker::Lorem.sentence,
        description: Faker::Lorem.paragraph
    )
  end

  describe 'querying the data-set' do
    let(:data_frame) { record_klass.all.as_data_frame }

    it 'should allow us to query the original record' do
      expect(data_frame.first).to eq(record)
    end

    it 'should allow us to query for column sub-sets' do
      expect(data_frame.select(:value, :description).first).to eq(record_klass.new(value: record.value, description: record.description))
    end

    it 'supports raw sql queries' do
      expect(data_frame.sql("SELECT rk_key, ck_value FROM #{data_frame.table_name}").first).to eq(record_klass.new(key: record.key, value: record.value))
    end

    describe 'aggregation' do
      let(:key) { record.key }
      let(:key_two) { Faker::Lorem.sentence }
      let(:query) { data_frame.select(:key, :* => {aggregate: :count, as: :count}).group(:key) }

      before do
        5.times { record_klass.create(key: key, value: Faker::Lorem.sentence, description: Faker::Lorem.paragraph) }
        5.times { record_klass.create(key: key_two, value: Faker::Lorem.sentence, description: Faker::Lorem.paragraph) }
      end

      it 'supports aggregation' do
        expect(query.get).to match_array([{key: key, count: 6}, {key: key_two, count: 5}])
      end
    end
  end
end