require 'scala_spec_helper'

describe LuaRDD do
  let(:model_klass) do
    generate_composite_model(:company, {id: :int, name: :text}, {value: :text}, {description: :text, uuid: :text})
  end

  let(:data_frame) { CassandraModel::Spark::DataFrame.from_csv(model_klass, 'ext/scala_helper/spec/fixtures/test_rdd.csv') }
  let(:schema) { data_frame.spark_data_frame.schema }
  let(:rdd) { data_frame.spark_data_frame.rdd }
  let(:lua_rdd) { LuaRDD.new(schema, rdd) }
  let(:result_lua_rdd) { lua_rdd }
  let(:spark_data_frame) { result_lua_rdd.toDF(data_frame.sql_context) }
  let(:new_data_frame) do
    CassandraModel::Spark::DataFrame.new(model_klass, nil, spark_data_frame: spark_data_frame, alias: Faker::Lorem.word)
  end

  subject { new_data_frame }

  describe '#toDF' do
    its(:request) { is_expected.to eq(data_frame.request) }
  end

  describe '#map' do
    let(:new_schema) { CassandraModel::Spark::SqlSchema.new(hello: :text, age: :int).schema }
    let(:result_lua_rdd) { lua_rdd.map(new_schema, "return {'Bobby', 37}") }

    it { expect(subject.request.uniq).to eq([hello: 'Bobby', age: 37]) }
  end

  describe '#flatMap' do
    let(:new_schema) { CassandraModel::Spark::SqlSchema.new(dumped_value: :text).schema }
    let(:script) do
      %q{local name = row.append(row.new(), 'dumped_value', ROW.name)
         local description = row.append(row.new(), 'dumped_value', ROW.description)
         return {name, description}
      }
    end
    let(:result_lua_rdd) { lua_rdd.flatMap(new_schema, script) }
    let(:result_values) { subject.request.map(&:values).map(&:first) }
    let(:expected_values) do
      data_frame.select(:name, :description).get.map(&:attributes).map(&:values).flatten
    end

    it { expect(result_values).to match_array(expected_values) }
  end

  describe '#filter' do
    let(:result_lua_rdd) { lua_rdd.filter("return tonumber(ROW.id) < 3") }

    it { expect(subject.request.uniq).to eq(data_frame.where(:id.lt => 3).get) }
  end
end