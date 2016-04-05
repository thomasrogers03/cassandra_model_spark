require 'spec_helper'

module CassandraModel
  module Spark
    describe Schema do

      let(:sql_schema) { Lib::SqlStructType.apply([]) }
      let(:cassandra_schema) { Schema.new(sql_schema) }

      describe '#schema' do
        let(:expected_schema) { {} }

        subject { cassandra_schema.schema }

        it { is_expected.to eq(expected_schema) }

        context 'with some fields' do
          let(:column) { Faker::Lorem.word.to_sym }
          let(:type) { Lib::SqlStringType }
          let(:sql_schema) { Lib::SqlStructType.apply([Lib::SqlStructField.apply(column, type, true, nil)]) }
          let(:expected_schema) { {column => :text} }

          it { is_expected.to eq(expected_schema) }

          context 'with multiple fields' do
            let(:column_two) { "#{Faker::Lorem.word}_#{Faker::Lorem.word}".to_sym }
            let(:type_two) { Lib::SqlIntegerType }
            let(:sql_schema) do
              Lib::SqlStructType.apply([
                                           Lib::SqlStructField.apply(column, type, true, nil),
                                           Lib::SqlStructField.apply(column_two, type_two, true, nil)
                                       ])
            end
            let(:expected_schema) { {column => :text, column_two => :int} }

            it { is_expected.to eq(expected_schema) }
          end

          shared_examples_for 'mapping a type to a sql type' do |cassandra_type, sql_type|
            let(:type) { sql_type }
            let(:expected_schema) { {column => cassandra_type} }
            it { is_expected.to eq(expected_schema) }
          end

          it_behaves_like 'mapping a type to a sql type', :double, Lib::SqlDoubleType
          it_behaves_like 'mapping a type to a sql type', :int, Lib::SqlIntegerType
          it_behaves_like 'mapping a type to a sql type', :timestamp, Lib::SqlTimestampType
          it_behaves_like 'mapping a type to a sql type', :blob, Lib::SqlBinaryType
          it_behaves_like 'mapping a type to a sql type', [:list, :int], Lib::SqlArrayType.apply(Lib::SqlIntegerType)
          it_behaves_like 'mapping a type to a sql type', [:list, :text], Lib::SqlArrayType.apply(Lib::SqlStringType)
          it_behaves_like 'mapping a type to a sql type', [:map, :int, :text], Lib::SqlMapType.apply(Lib::SqlIntegerType, Lib::SqlStringType, true)
          it_behaves_like 'mapping a type to a sql type', [:map, :text, :int], Lib::SqlMapType.apply(Lib::SqlStringType, Lib::SqlIntegerType, true)
        end
      end

      describe '#==' do
        let(:column) { Faker::Lorem.word.to_sym }
        let(:type) { Lib::SqlStringType }
        let(:sql_schema) { Lib::SqlStructType.apply([Lib::SqlStructField.apply(column, type, true, nil)]) }
        let(:sql_schema_two) { sql_schema }
        let(:cassandra_schema_two) { Schema.new(sql_schema_two) }

        subject { cassandra_schema }

        it { is_expected.to eq(cassandra_schema_two) }

        context 'with different types' do
          let(:cassandra_schema_two) { sql_schema_two }

          it { is_expected.not_to eq(cassandra_schema_two) }
        end

        context 'with different schemas' do
          let(:sql_schema_two) { Lib::SqlStructType.apply([]) }

          it { is_expected.not_to eq(cassandra_schema_two) }
        end
      end

    end
  end
end