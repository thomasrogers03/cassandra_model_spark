require 'scala_spec_helper'

describe LuaRDD do
  let(:model_klass) do
    Class.new(CassandraModel::Record) do
      extend CassandraModel::DataModelling

      def self.name
        Faker::Lorem.word
      end

      model_data do |inquirer, data_set|
        inquirer.knows_about(:id)
        data_set.is_defined_by(:name)
        data_set.knows_about(:description)
      end
    end
  end

  let(:data_frame) { CassandraModel::Spark::DataFrame.from_csv(model_klass, 'ext/scala_helper/spec/fixtures/test_rdd.csv') }
  let(:schema) { data_frame.spark_data_frame.schema }
  let(:rdd) { data_frame.spark_data_frame.rdd }
  let(:lua_rdd) { LuaRDD.new(schema, rdd) }

  subject { lua_rdd }

  describe '#toDF' do
    let(:spark_data_frame) { lua_rdd.toDF(data_frame.sql_context) }
    let(:new_data_frame) { CassandraModel::Spark::DataFrame.new(model_klass, nil, spark_data_frame: spark_data_frame, alias: Faker::Lorem.word) }

    subject { new_data_frame }

    its(:request) { is_expected.to eq(data_frame.request) }
  end
end