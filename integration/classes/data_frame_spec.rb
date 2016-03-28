require 'integration_spec_helper'

describe 'querying a DataFrame' do
  let(:record_klass) { composite_record_klass }
  let!(:record) do
    record_klass.create(
        key: Faker::Lorem.word,
        value: Faker::Lorem.sentence,
        description: Faker::Lorem.paragraph,
        extra_data: nil
    )
  end

  describe 'querying the data-set' do
    let(:data_frame) { record_klass.all.as_data_frame }

    it 'should allow us to query the original record' do
      expect(data_frame.first).to eq(record)
    end
  end
end