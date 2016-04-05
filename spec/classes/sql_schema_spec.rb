require 'spec_helper'

module CassandraModel
  module Spark
    describe SqlSchema do

      let(:schema) { {} }
      let(:sql_schema) { SqlSchema.new(schema) }

      describe '#schema' do
        let(:expected_fields) { [] }
        let(:expected_schema) { Lib::SqlStructType.apply(expected_fields) }

        subject { sql_schema.schema }

        it { is_expected.to eq(expected_schema) }

        context 'with some fields' do
          let(:column) { Faker::Lorem.word.to_sym }
          let(:type) { :text }
          let(:schema) { {column => type} }
          let(:expected_fields) { [Lib::SqlStructField.apply(column.to_s, Lib::SqlStringType, true, Lib::SqlMetadata.empty)] }

          it { is_expected.to eq(expected_schema) }

          context 'with multiple fields' do
            let(:column_two) { "#{Faker::Lorem.word}_#{Faker::Lorem.word}".to_sym }
            let(:type_two) { :int }
            let(:schema) { {column => type, column_two => type_two} }
            let(:expected_fields) do
              [
                  Lib::SqlStructField.apply(column.to_s, Lib::SqlStringType, true, Lib::SqlMetadata.empty),
                  Lib::SqlStructField.apply(column_two.to_s, Lib::SqlIntegerType, true, Lib::SqlMetadata.empty)
              ]
            end

            it { is_expected.to eq(expected_schema) }
          end

          shared_examples_for 'mapping a type to a sql type' do |source_type, sql_type|
            let(:type) { source_type }
            let(:expected_fields) { [Lib::SqlStructField.apply(column.to_s, sql_type, true, Lib::SqlMetadata.empty)] }
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
        let(:type) { :text }
        let(:schema) { {column => type} }
        let(:schema_two) { schema }
        let(:sql_schema_two) { SqlSchema.new(schema_two) }

        subject { sql_schema }

        it { is_expected.to eq(sql_schema_two) }

        context 'with different types' do
          let(:sql_schema_two) { schema_two }

          it { is_expected.not_to eq(sql_schema_two) }
        end

        context 'with different schemas' do
          let(:sql_schema_two) { {} }

          it { is_expected.not_to eq(sql_schema_two) }
        end
      end

    end
  end
end