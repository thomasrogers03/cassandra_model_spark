require 'spec_helper'

module CassandraModel
  module Spark
    describe Schema do

      let(:schema) { SqlStructType.apply([]) }
      let(:sql_schema) { Schema.new(schema) }

      describe '#schema' do
        let(:expected_schema) { {} }

        subject { sql_schema.schema }

        it { is_expected.to eq(expected_schema) }

        context 'with some fields' do
          let(:column) { Faker::Lorem.word.to_sym }
          let(:type) { SqlStringType }
          let(:schema) { SqlStructType.apply([SqlStructField.apply(column, type, true, nil)]) }
          let(:expected_schema) { {column => :text} }

          it { is_expected.to eq(expected_schema) }

          context 'with multiple fields' do
            let(:column_two) { "#{Faker::Lorem.word}_#{Faker::Lorem.word}".to_sym }
            let(:type_two) { SqlIntegerType }
            let(:schema) do
              SqlStructType.apply([
                                      SqlStructField.apply(column, type, true, nil),
                                      SqlStructField.apply(column_two, type_two, true, nil)
                                  ])
            end
            let(:expected_schema) { {column => :text, column_two => :int} }

            it { is_expected.to eq(expected_schema) }
          end

          shared_examples_for 'mapping a type to a sql type' do |cassandra_type, sql_type|
            let(:type) { sql_type }
            let(:expected_schema) { { column => cassandra_type } }
            it { is_expected.to eq(expected_schema) }
          end

          it_behaves_like 'mapping a type to a sql type', :double, SqlDoubleType
          it_behaves_like 'mapping a type to a sql type', :int, SqlIntegerType
          it_behaves_like 'mapping a type to a sql type', :timestamp, SqlTimestampType
          it_behaves_like 'mapping a type to a sql type', :blob, SqlBinaryType
          it_behaves_like 'mapping a type to a sql type', [:list, :int], SqlArrayType.apply(SqlIntegerType, true)
          it_behaves_like 'mapping a type to a sql type', [:list, :text], SqlArrayType.apply(SqlStringType, true)
          it_behaves_like 'mapping a type to a sql type', [:map, :int, :text], SqlMapType.apply(SqlIntegerType, SqlStringType, true)
          it_behaves_like 'mapping a type to a sql type', [:map, :text, :int], SqlMapType.apply(SqlStringType, SqlIntegerType, true)
        end
      end

    end
  end
end