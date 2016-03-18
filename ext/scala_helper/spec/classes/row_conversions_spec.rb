require 'scala_spec_helper'

describe SqlRowConversions do
  let(:model_klass) do
    Class.new(CassandraModel::Record) do
      extend CassandraModel::DataModelling

      def self.name
        Faker::Lorem.word
      end

      self.table_name = :attrhist

      model_data do |inquirer, data_set|
        inquirer.knows_about(:id)
        inquirer.change_type_of(:id).to(:int)

        data_set.guess_data_types!
        data_set.is_defined_by(:name)
        data_set.knows_about(:attribute_id, :created_at, :created_at_id, :description, :price, :year)
      end

      table.allow_truncation!
      table.truncate!
    end
  end

  let(:now) { Time.now.beginning_of_week }
  let(:id) { rand(0...10) }
  let(:name) { Faker::Lorem.word }
  let(:description) { Faker::Lorem.sentence }
  let(:year) { rand(1990...2000) }
  let(:price) { Faker::Commerce.price }
  let(:time_gen) { Cassandra::Uuid::Generator.new }
  let(:timeuuid) { time_gen.now }
  let(:attribute_id) { Cassandra::Uuid.new(SecureRandom.uuid) }

  before do
    model_klass.create(
        id: id,
        name: name,
        description: description,
        year: year,
        price: price,
        created_at: now,
        created_at_id: timeuuid,
        attribute_id: attribute_id
    )
  end

  describe '#cassandraRDDToRowRDD' do
    let(:rdd) { model_klass.rdd }
    let(:result_rdd) { SqlRowConversions.cassandraRDDToRowRDD(rdd) }

    subject { result_rdd.first }

    it { expect(subject.get(0).toString.to_i).to eq(id) }
    it { expect(subject.get(1).toString).to eq(name) }

    it { expect(subject.get(2).toString).to eq(attribute_id.to_s) }
    it { expect(subject.get(2).getClass.getSimpleName).to eq('String') }

    it { expect(subject.get(3).getTime).to eq((now.to_f * 1000).to_i) }
    it { expect(subject.get(3).getClass.getSimpleName).to eq('Timestamp') }

    it { expect(subject.get(4).toString).to eq(timeuuid.to_s) }
    it { expect(subject.get(4).getClass.getSimpleName).to eq('String') }

    it { expect(subject.get(5).toString).to eq(description) }
    it { expect(subject.get(6).toString.to_f).to eq(price) }
    it { expect(subject.get(7).toString.to_i).to eq(year) }
  end
end