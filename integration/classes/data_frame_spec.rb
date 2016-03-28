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
  end
end