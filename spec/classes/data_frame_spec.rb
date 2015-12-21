require 'rspec'

module CassandraModel
  module Spark
    describe DataFrame do
      let(:cassandra_columns) { {partition: :text} }
      let(:table_name) { Faker::Lorem.word }
      let(:table) { TableRedux.new(table_name) }
      let(:record_klass_rdd_mapper) { nil }
      let(:record_klass) do
        double(:klass, table: table, cassandra_columns: cassandra_columns, table_name: table_name, rdd_row_mapping: record_klass_rdd_mapper)
      end
      let(:rdd) { double(:rdd) }
      let(:data_frame) { DataFrame.new(record_klass, rdd) }

      let(:frame_context) { double(:sql_context) }
      let(:spark_frame) { double(:frame, sql_context: frame_context, register_temp_table: nil) }
      let(:spark_frame_alias) { Faker::Lorem.word }

      before do
        allow(record_klass).to(receive(:normalized_column)) { |column| column }
        allow(record_klass).to(receive(:select_column)) { |column| column }
        allow(record_klass).to(receive(:select_columns)) do |columns|
          columns.map { |column| record_klass.select_column(column) }
        end
      end

      describe 'initialization' do
        context 'when a spark data frame is provided to the initializer' do
          let(:data_frame) { DataFrame.new(record_klass, nil, spark_data_frame: spark_frame, alias: spark_frame_alias) }

          it 'should register a temp table with the specified alias' do
            expect(spark_frame).to receive(:register_temp_table).with(spark_frame_alias)
            data_frame
          end

          context 'when the alias is a symbol' do
            let(:spark_frame_alias) { Faker::Lorem.word.to_sym }

            it 'should convert it to a string' do
              expect(spark_frame).to receive(:register_temp_table).with(spark_frame_alias.to_s)
              data_frame
            end
          end

          context 'when an alias is not provided' do
            let(:spark_frame_alias) { nil }

            it 'should raise an error' do
              expect { data_frame }.to raise_error(ArgumentError, 'DataFrames created from Spark DataFrames require aliases!')
            end
          end
        end
      end

      describe '#record_klass' do
        subject { data_frame.record_klass }

        it { is_expected.to eq(record_klass) }
      end

      describe '#table_name' do
        subject { data_frame.table_name }

        it { is_expected.to eq(table_name) }

        context 'with a specific table name' do
          let(:alias_table_name) { Faker::Lorem.word }
          let(:data_frame) { DataFrame.new(record_klass, rdd, alias: alias_table_name) }

          it { is_expected.to eq(alias_table_name) }
        end
      end

      describe '#union' do
        let(:record_klass_two) { record_klass }
        let(:rdd_two) { double(:rdd) }
        let(:union_rdd) { double(:rdd) }
        let(:data_frame_two) { DataFrame.new(record_klass_two, rdd_two) }
        let(:union_data_frame) { DataFrame.new(record_klass, union_rdd) }

        before { allow(rdd).to receive(:union).with(rdd_two).and_return(union_rdd) }

        subject { data_frame.union(data_frame_two) }

        it { is_expected.to eq(union_data_frame) }

        context 'when the Record classes do not match' do
          let(:record_klass_two) { double(:klass, table_name: Faker::Lorem.word, rdd_row_mapping: nil) }

          it { expect { subject }.to raise_error(ArgumentError, 'Cannot union DataFrames with different Record types!') }
        end
      end

      describe '#sql_context' do
        let(:spark_context) { record_klass.table.connection.spark_context }
        let(:keyspace) { Faker::Lorem.word }
        subject { data_frame.sql_context }
        before { record_klass.table.connection.config = {keyspace: keyspace} }

        it { is_expected.to eq(CassandraSQLContext.new(spark_context)) }

        it 'should cache the value' do
          data_frame.sql_context
          expect(CassandraSQLContext).not_to receive(:new)
          data_frame.sql_context
        end

        it 'should set the keyspace from the record class' do
          expect(subject.keyspace).to eq(keyspace)
        end

        context 'when the sql context is specified through the initializer' do
          let(:sql_context) { double(:sql_context) }
          let(:data_frame) { DataFrame.new(record_klass, rdd, sql_context: sql_context) }

          it { is_expected.to eq(sql_context) }
        end

        context 'when a spark data frame is provided to the initializer' do
          let(:data_frame) { DataFrame.new(record_klass, nil, spark_data_frame: spark_frame, alias: spark_frame_alias) }

          it { is_expected.to eq(frame_context) }

          context 'when the record klass has a row mapper' do
            let(:row_mapper) { double(:row_mapper) }
            let(:rdd_mapper) { {mapper: row_mapper, type_map: {}} }
            let(:record_klass_rdd_mapper) { rdd_mapper }

            it 'should not use the row mapper of the class' do
              expect(row_mapper).not_to receive(:mappedRDD)
              subject
            end
          end
        end
      end

      describe '#spark_data_frame' do
        let(:sql_columns) { {'partition' => SqlStringType} }
        subject { data_frame.spark_data_frame }

        it { is_expected.to be_a_kind_of(SqlDataFrame) }

        it 'should instance-cache the frame' do
          data_frame.spark_data_frame
          expect(SqlDataFrame).not_to receive(:new)
          data_frame.spark_data_frame
        end

        it 'should register a temp table with the name of the table associated with the frame' do
          expect_any_instance_of(SqlDataFrame).to receive(:register_temp_table).with(table_name)
          data_frame.spark_data_frame
        end

        context 'when the table name is a symbol' do
          let(:table_name) { Faker::Lorem.word.to_sym }

          it 'should register a temp using the stringified table name' do
            expect_any_instance_of(SqlDataFrame).to receive(:register_temp_table).with(table_name.to_s)
            data_frame.spark_data_frame
          end
        end

        context 'when a spark data frame is provided to the initializer' do
          let(:data_frame) { DataFrame.new(record_klass, nil, spark_data_frame: spark_frame, alias: spark_frame_alias) }

          it { is_expected.to eq(spark_frame) }
        end

        context 'with a specific table name specified' do
          let(:alias_table_name) { Faker::Lorem.word }
          let(:data_frame) { DataFrame.new(record_klass, rdd, alias: alias_table_name) }

          it 'should register a temp table with the alias' do
            expect_any_instance_of(SqlDataFrame).to receive(:register_temp_table).with(alias_table_name)
            data_frame.spark_data_frame
          end
        end

        its(:schema) { is_expected.to eq(sql_columns) }

        context 'with a different set of columns' do
          let(:cassandra_columns) { {partition: :text, clustering: :int} }
          let(:sql_columns) { {'partition' => SqlStringType, 'clustering' => SqlIntegerType} }

          its(:schema) { is_expected.to eq(sql_columns) }
        end

        shared_examples_for 'mapping a cassandra column type to a spark sql type' do |cassandra_type, sql_type|
          let(:cassandra_columns) { {partition: cassandra_type} }
          let(:sql_columns) { {'partition' => sql_type} }

          its(:schema) { is_expected.to eq(sql_columns) }
        end

        it_behaves_like 'mapping a cassandra column type to a spark sql type', :double, SqlDoubleType
        it_behaves_like 'mapping a cassandra column type to a spark sql type', :timestamp, SqlTimestampType

        shared_examples_for 'mapping an rdd' do
          let(:rdd_mapper) { {mapper: row_mapper, type_map: type_map} }
          let(:type_map) { nil }
          let(:mapped_rdd) { double(:rdd) }
          let(:row_mapper) { double(:mapper) }

          before { allow(row_mapper).to receive(:mappedRDD).with(rdd).and_return(mapped_rdd) }

          it 'should create the frame using the mapped rdd' do
            expect(subject.rdd).to eq(mapped_rdd)
          end

          context 'when a column type map is provided' do
            let(:mapped_column) { Faker::Lorem.word.to_sym }
            let(:mapped_column_alias) { Faker::Lorem.word.to_sym }
            let(:type_map) { {mapped_column => {type: SqlStringStringMapType, name: mapped_column_alias}} }
            let(:cassandra_columns) { {mapped_column => :blob} }
            let(:sql_columns) { {mapped_column_alias.to_s => SqlStringStringMapType} }

            its(:schema) { is_expected.to eq(sql_columns) }

            context 'when the Record class maps column names' do
              let(:cassandra_columns) { {"rk_#{mapped_column}" => :blob} }
              let(:sql_columns) { {mapped_column_alias.to_s => SqlStringStringMapType} }

              before do
                allow(record_klass).to(receive(:normalized_column)) do |column|
                  column.match(/^rk_(.+)$/)[1].to_sym
                end
              end

              its(:schema) { is_expected.to eq(sql_columns) }
            end
          end
        end

        context 'when a row mapper is provided' do
          let(:options) { {row_mapping: rdd_mapper} }
          let(:data_frame) { DataFrame.new(record_klass, rdd, options) }

          it_behaves_like 'mapping an rdd'
        end

        context 'when the record klass has a row mapper' do
          let(:record_klass_rdd_mapper) { rdd_mapper }

          it_behaves_like 'mapping an rdd'
        end
      end

      describe '#cached' do
        it 'should yield' do
          expect { |block| data_frame.cached(&block) }.to yield_control
        end

        it 'should yield within context of the frame' do
          frame = nil
          data_frame.cached { frame = self }
          expect(frame).to eq(data_frame)
        end

        it 'should cache the data frame' do
          cached = nil
          data_frame.cached { cached = spark_data_frame.cached? }
          expect(cached).to eq(true)
        end

        it 'should uncache it afterwards' do
          data_frame.cached {}
          expect(data_frame.spark_data_frame).not_to be_cached
        end
      end

      describe 'querying the data frame' do
        subject { data_frame }

        it_behaves_like 'a query helper'
      end

      shared_examples_for 'an async method not yet implemented' do |method|
        describe "##{method}" do
          subject { data_frame.public_send(method, {}, {}) }

          it { expect { subject.get }.to raise_error(NotImplementedError) }
        end
      end

      describe '#request_async' do
        subject { data_frame.request_async({}) }
        it { is_expected.to be_a_kind_of(ResultPaginator) }
      end

      it_behaves_like 'an async method not yet implemented', :request_async
      it_behaves_like 'an async method not yet implemented', :first_async

      describe '#query' do
        let(:sql_context) { double(:sql_context) }
        let(:query) { double(:query) }
        let(:restriction) { {} }
        let(:options) { {} }
        let(:query_sql) { "SELECT * FROM #{table_name}" }
        let(:data_frame) { DataFrame.new(record_klass, rdd, sql_context: sql_context) }

        subject { data_frame.query(restriction, options) }

        before do
          allow(sql_context).to receive(:sql).with(query_sql).and_return(query)
        end

        it { is_expected.to eq(query) }

        it 'should ensure that the data_frame has been created' do
          expect(data_frame).to receive(:spark_data_frame)
          subject
        end

        describe 'restricting the data set' do
          let(:restriction) { {partition: 47} }
          let(:query_sql) { "SELECT * FROM #{table_name} WHERE `partition` = 47" }

          it { is_expected.to eq(query) }

          context 'with a multi-column restriction' do
            let(:restriction) { {partition: 30, clustering: 20.0} }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `partition` = 30 AND `clustering` = 20.0" }

            it { is_expected.to eq(query) }
          end

          context 'when the restriction contains strings' do
            let(:restriction) { {partition: 'part'} }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `partition` = 'part'" }

            it { is_expected.to eq(query) }
          end

          context 'when the restriction contains a timestamp' do
            let(:time) { Time.now }
            let(:restriction) { {partition: time} }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `partition` = '#{time}'" }

            it { is_expected.to eq(query) }
          end

          context 'when the key is a KeyComparer' do
            let(:restriction) { {:price.gt => 50.49} }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `price` > 50.49" }

            it { is_expected.to eq(query) }

            context 'when the columns are mapped' do
              let(:query_sql) { "SELECT * FROM #{table_name} WHERE `ck_price` > 50.49" }
              before { allow(record_klass).to(receive(:select_column)) { |column| :"ck_#{column}" } }

              it { is_expected.to eq(query) }
            end

            context 'when provided with a child column' do
              let(:child_key) { Faker::Lorem.word.to_sym }
              let(:restriction) { {:price.child(child_key).lt => 43.99} }
              let(:query_sql) { "SELECT * FROM #{table_name} WHERE `price`.`#{child_key}` < 43.99" }

              it { is_expected.to eq(query) }

              context 'when the columns are mapped' do
                let(:query_sql) { "SELECT * FROM #{table_name} WHERE `ck_price`.`#{child_key}` < 43.99" }
                before { allow(record_klass).to(receive(:select_column)) { |column| :"ck_#{column}" } }

                it { is_expected.to eq(query) }
              end
            end
          end

          context 'when the columns are mapped' do
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `ck_partition` = 47" }
            before { allow(record_klass).to(receive(:select_column)) { |column| :"ck_#{column}" } }

            it { is_expected.to eq(query) }
          end

          context 'when provided with a child column' do
            let(:child_key) { Faker::Lorem.word.to_sym }
            let(:restriction) { {:partition.child(child_key) => 49.99} }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `partition`.`#{child_key}` = 49.99" }

            it { is_expected.to eq(query) }

            context 'when the columns are mapped' do
              let(:query_sql) { "SELECT * FROM #{table_name} WHERE `ck_partition`.`#{child_key}` = 49.99" }
              before { allow(record_klass).to(receive(:select_column)) { |column| :"ck_#{column}" } }

              it { is_expected.to eq(query) }
            end
          end
        end

        shared_examples_for 'a column grouping' do |method, clause|
          let(:options) { {method => [:partition]} }
          let(:query_sql) { "SELECT * FROM #{table_name} #{clause} `partition`" }

          it { is_expected.to eq(query) }

          context 'with multiple columns' do
            let(:options) { {method => [:partition, :clustering]} }
            let(:query_sql) { "SELECT * FROM #{table_name} #{clause} `partition`, `clustering`" }

            it { is_expected.to eq(query) }
          end

          context 'when the columns are mapped' do
            let(:query_sql) { "SELECT * FROM #{table_name} #{clause} `rk_partition`" }

            before do
              allow(record_klass).to(receive(:select_column)) { |column| :"rk_#{column}" }
            end

            it { is_expected.to eq(query) }
          end

          context 'when provided with a child column' do
            let(:child_key) { Faker::Lorem.word.to_sym }
            let(:options) { {method => [:partition.child(child_key)]} }
            let(:query_sql) { "SELECT * FROM #{table_name} #{clause} `partition`.`#{child_key}`" }

            it { is_expected.to eq(query) }
          end

          context 'when provided with a multiple child columns' do
            let(:child_key) { Faker::Lorem.word.to_sym }
            let(:child_key_two) { Faker::Lorem.word.to_sym }
            let(:options) { {method => [:partition.child(child_key), :partition.child(child_key_two)]} }
            let(:query_sql) { "SELECT * FROM #{table_name} #{clause} `partition`.`#{child_key}`, `partition`.`#{child_key_two}`" }

            it { is_expected.to eq(query) }
          end
        end

        it_behaves_like 'a column grouping', :group, 'GROUP BY'
        it_behaves_like 'a column grouping', :order_by, 'ORDER BY'

        describe 'ordering column in specific directions' do
          let(:direction) { :desc }
          let(:options) { {order_by: [{partition: direction}]} }
          let(:query_sql) { "SELECT * FROM #{table_name} ORDER BY `partition` DESC" }

          it { is_expected.to eq(query) }

          context 'with a different direction' do
            let(:direction) { :asc }
            let(:query_sql) { "SELECT * FROM #{table_name} ORDER BY `partition` ASC" }

            it { is_expected.to eq(query) }
          end

          context 'when provided with a child column' do
            let(:child_key) { Faker::Lorem.word.to_sym }
            let(:options) { {order_by: [:partition.child(child_key) => :desc]} }
            let(:query_sql) { "SELECT * FROM #{table_name} ORDER BY `partition`.`#{child_key}` DESC" }

            it { is_expected.to eq(query) }
          end

          context 'when provided with a multiple child columns' do
            let(:child_key) { Faker::Lorem.word.to_sym }
            let(:child_key_two) { Faker::Lorem.word.to_sym }
            let(:options) { {order_by: [{:partition.child(child_key) => :asc}, {:partition.child(child_key_two) => :desc}]} }
            let(:query_sql) { "SELECT * FROM #{table_name} ORDER BY `partition`.`#{child_key}` ASC, `partition`.`#{child_key_two}` DESC" }

            it { is_expected.to eq(query) }
          end
        end

        context 'with a limit specified' do
          let(:limit) { rand(1...5) }
          let(:options) { {limit: limit} }
          let(:query_sql) { "SELECT * FROM #{table_name} LIMIT #{limit}" }

          it { is_expected.to eq(query) }
        end

        context 'with a different columns selected' do
          let(:options) { {select: [:partition]} }
          let(:query_sql) { "SELECT `partition` FROM #{table_name}" }

          it { is_expected.to eq(query) }

          context 'when explicitly selecting all columns (ie for an aggregate)' do
            let(:options) { {select: [:*]} }
            let(:query_sql) { "SELECT * FROM #{table_name}" }

            it { is_expected.to eq(query) }
          end

          context 'with multiple columns' do
            let(:options) { {select: [:partition, :clustering]} }
            let(:query_sql) { "SELECT `partition`, `clustering` FROM #{table_name}" }

            it { is_expected.to eq(query) }
          end

          context 'when the columns are mapped' do
            let(:query_sql) { "SELECT `rk_partition` FROM #{table_name}" }

            before do
              allow(record_klass).to(receive(:select_column)) { |column| :"rk_#{column}" }
            end

            it { is_expected.to eq(query) }

            context 'when columns are aliased' do
              let(:options) { {select: [{partition: {as: :part}}]} }
              let(:query_sql) { "SELECT `rk_partition` AS part FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end
          end

          context 'when columns are aliased' do
            let(:options) { {select: [{partition: {as: :part}}]} }
            let(:query_sql) { "SELECT `partition` AS part FROM #{table_name}" }

            it { is_expected.to eq(query) }
          end

          describe 'dealing with column children' do
            let(:child_key) { Faker::Lorem.word.to_sym }

            context 'when provided with a child column' do
              let(:options) { {select: [:partition.child(child_key)]} }
              let(:query_sql) { "SELECT `partition`.`#{child_key}` FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            context 'when provided with a multiple child columns' do
              let(:child_key_two) { Faker::Lorem.word.to_sym }
              let(:options) { {select: [:partition.child(child_key), :partition.child(child_key_two)]} }
              let(:query_sql) { "SELECT `partition`.`#{child_key}`, `partition`.`#{child_key_two}` FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            describe 'aggregating on child columns' do
              let(:options) { {select: [:partition.child(child_key) => {aggregate: :count}]} }
              let(:query_sql) { "SELECT COUNT(`partition`.`#{child_key}`) FROM #{table_name}" }

              it { is_expected.to eq(query) }

              context 'when using the short-hand' do
                let(:options) { {select: [:partition.child(child_key) => :count]} }
                let(:query_sql) { "SELECT COUNT(`partition`.`#{child_key}`) AS partition_#{child_key}_count FROM #{table_name}" }

                it { is_expected.to eq(query) }
              end
            end
          end

          context 'when the column is to be aggregated' do
            let(:aggregate) { :avg }
            let(:options) { {select: [{partition: {aggregate: aggregate}}]} }
            let(:query_sql) { "SELECT AVG(`partition`) FROM #{table_name}" }

            it { is_expected.to eq(query) }

            context 'when the aggregate is provided using short-hand' do
              let(:select_key) { Faker::Lorem.word.to_sym }
              let(:aggregate) { %w(avg sum min max).sample.to_sym }
              let(:options) { {select: [select_key => aggregate]} }
              let(:query_sql) { "SELECT #{aggregate.upcase}(`#{select_key}`) AS #{select_key}_#{aggregate} FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            shared_examples_for 'an aggregate function' do |function|
              let(:aggregate) { function.downcase.to_sym }
              let(:sql_aggregate) { function.to_s.upcase }
              let(:query_sql) { "SELECT #{sql_aggregate}(`partition`) FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            it_behaves_like 'an aggregate function', :count
            it_behaves_like 'an aggregate function', :sum

            shared_examples_for 'casting as column to another type' do |type|
              let(:aggregate) { :"cast_#{type}" }
              let(:query_sql) { "SELECT CAST(`partition` AS #{type}) FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            it_behaves_like 'casting as column to another type', :int
            it_behaves_like 'casting as column to another type', :double
            it_behaves_like 'casting as column to another type', :string

            context 'when requesting a distinct aggregate' do
              let(:aggregate) { :distinct }
              let(:query_sql) { "SELECT DISTINCT `partition` FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            context 'when requesting a distinct count aggregate' do
              let(:aggregate) { :count_distinct }
              let(:query_sql) { "SELECT COUNT(DISTINCT `partition`) FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            context 'when requesting a variance aggregate' do
              let(:aggregate) { :variance }
              let(:query_sql) { "SELECT AVG(POW(`partition`,2)) - POW(AVG(`partition`),2) FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            context 'when requesting a standard deviation aggregate' do
              let(:aggregate) { :stddev }
              let(:query_sql) { "SELECT POW(AVG(POW(`partition`,2)) - POW(AVG(`partition`),2),0.5) FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end
          end
        end

        describe 'pulling data from the data set' do
          shared_examples_for 'a method mapping a query result to a Record' do |method, collect_method|
            let(:result_sql_type) { SqlTypeWrapper.new(SqlStringType) }
            let(:result_value) { Faker::Lorem.word }

            let(:key) { Faker::Lorem.word }
            let(:attributes) { {key => Faker::Lorem.word} }
            let(:result) { {select_key => result_value} }
            let(:select_key) { Faker::Lorem.word }
            let(:options) { {select: [select_key]} }
            let(:available_columns) { [select_key.to_sym] }
            let(:deferred_columns) { [] }

            let(:fields) { [SqlStructField.new(select_key, result_sql_type)] }
            let(:query_schema) { SqlStructType.new(fields) }
            let(:query) { double(:query, schema: query_schema, first: RDDRow[result], collect: [RDDRow[result]]) }
            let(:record_attributes) { {select_key.to_sym => result_value} }

            before do
              allow(data_frame).to receive(:query).with(attributes, options).and_return(query)
              allow(record_klass).to receive(:new) do |attributes|
                MockRecord.new(attributes)
              end
              allow(record_klass).to receive(:normalized_attributes) do |attributes|
                attributes.symbolize_keys
              end
              allow(record_klass).to receive(:columns).and_return(available_columns)
              allow(record_klass).to receive(:deferred_columns).and_return(deferred_columns)
            end

            it 'should support default values' do
              allow(data_frame).to receive(:query).and_return(query)
              expect { data_frame.public_send(method) }.not_to raise_error
            end

            it 'should return the result mapped to a CassandraModel::Record' do
              expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
            end

            shared_examples_for 'converting sql types back to ruby types' do |value, sql_type|
              let(:result_sql_type) { SqlTypeWrapper.new(sql_type) }
              let(:result_value) { value }

              it 'should return the result mapped to a CassandraModel::Record' do
                expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
              end
            end

            it_behaves_like 'converting sql types back to ruby types', 15, SqlIntegerType
            it_behaves_like 'converting sql types back to ruby types', SqlLong.new(153), SqlLongType
            it_behaves_like 'converting sql types back to ruby types', 15.3, SqlDoubleType
            it_behaves_like 'converting sql types back to ruby types', Time.at(12544), SqlTimestampType
            it_behaves_like 'converting sql types back to ruby types', {'hello' => 'world'}, SqlStringStringMapType

            context 'when a type is a StructType' do
              let(:sql_type) { SqlStructType.new([SqlStructField.new('description', SqlStringType)]) }
              let(:result_sql_type) { sql_type }
              let(:result_value) { RDDRow[description: Faker::Lorem.word] }

              it 'should recursively map the results' do
                expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
              end
            end

            context 'with a type we cannot handle' do
              let(:result_sql_type) { SqlTypeWrapper.new(SqlFakeType) }
              let(:result_value) { '1239333-33333' }

              it 'should convert to a string' do
                expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
              end
            end

            context 'when the record maps result columns' do
              let(:result) { {"rk_#{select_key}" => result_value} }
              let(:fields) { [SqlStructField.new("rk_#{select_key}", result_sql_type)] }

              before do
                allow(record_klass).to receive(:normalized_attributes) do |attributes|
                  attributes.inject({}) do |memo, (key, value)|
                    memo.merge!(key.match(/^rk_(.+)$/)[1].to_sym => value)
                  end
                end
              end

              it 'should map the columns' do
                expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
              end
            end

            context 'when a returned column is not part of the cassandra table' do
              let(:available_columns) { [Faker::Lorem.word.to_sym] }

              it 'should return a hash instead of the Record class' do
                expect(data_frame.public_send(method, attributes, options)).to include(record_attributes)
              end

              context 'when it is part of the deferred columns' do
                let(:available_columns) { [Faker::Lorem.word.to_sym] }
                let(:deferred_columns) { [select_key.to_sym] }

                it 'should give back a record instance' do
                  expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
                end
              end
            end
          end

          describe '#first' do
            let(:record_result) { MockRecord.new(record_attributes) }
            it_behaves_like 'a method mapping a query result to a Record', :first, :first
          end

          describe '#request' do
            let(:record_result) { [MockRecord.new(record_attributes)] }
            it_behaves_like 'a method mapping a query result to a Record', :request, :collect
          end

        end
      end
    end
  end
end
