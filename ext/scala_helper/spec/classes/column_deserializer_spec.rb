require 'scala_spec_helper'

module CassandraModel
  module Spark
    module Lib
      describe SparkColumnDeserializer do
        let(:rdd) { composite_record_klass.rdd }
        let(:extra) { Faker::Lorem.words }
        let!(:test_record) do
          composite_record_klass.create(
              key: Faker::Lorem.word,
              value: Faker::Lorem.word,
              description: Faker::Lorem.sentence,
              extra_data: 'MRSH' + Marshal.dump(extra),
          )
        end

        describe '.mappedRDD' do
          let(:column) { :extra_data }
          let(:column_index) { composite_record_klass.cassandra_columns.keys.find_index(column) }

          subject { SparkColumnDeserializer.mappedRDD(rdd, column_index).first.columnValues.apply(column_index).map(&:toString) }

          it { is_expected.to eq(extra) }

          context 'with an un-serialized column' do
            let(:column) { :description }

            it { expect { subject }.to raise_error }
          end
        end
      end
    end
  end
end