require 'spec_helper'

module CassandraModel
  module Spark
    describe DataFrame do
      let(:partition_key) { {partition: :text} }
      let(:clustering_columns) { {} }
      let(:record_fields) { {} }
      let(:cassandra_columns) { partition_key.merge(clustering_columns).merge(record_fields) }
      let(:table_name) { record_klass.table_name }
      let(:table) { TableRedux.new(table_name) }
      let(:record_klass_rdd_mapper) { nil }
      let(:deferred_columns) { [] }

      let(:record_klass) do
        generate_simple_model(
            Faker::Lorem.word,
            partition_key,
            clustering_columns,
            record_fields,
            record_klass_rdd_mapper,
            deferred_columns
        )
      end
      let(:composite_record_klass) do
        generate_composite_model(
            Faker::Lorem.word,
            partition_key,
            clustering_columns,
            record_fields,
            record_klass_rdd_mapper,
            deferred_columns
        )
      end

      let(:rdd) { double(:rdd) }
      let(:data_frame) { DataFrame.new(record_klass, rdd) }

      let(:frame_context) { double(:sql_context) }
      let(:spark_frame) { double(:frame, sql_context: frame_context, register_temp_table: nil) }
      let(:spark_frame_alias) { Faker::Lorem.word }

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

          it 'should save an attribute indicating that this is a derived DataFrame' do
            expect(data_frame).to be_derived
          end
        end
      end

      describe '.from_csv' do
        let(:path_extension) { %w(csv tsv txt tab bin blah stuff).sample }
        let(:path) { "#{Faker::Internet.url}.#{path_extension}" }
        let(:table_id) { SecureRandom.hex(2) }
        let(:table_name) { File.basename(path).gsub(/\./, '_') + "_#{table_id}" }
        let!(:csv_frame) { mock_csv_frame_load_variation(path) }
        let(:data_frame) { DataFrame.from_csv(record_klass, path) }

        subject { data_frame }

        before { allow(SecureRandom).to receive(:hex).with(2).and_return(table_id) }

        def mock_csv_frame_load_variation(path, read_options = {'header' => 'true'})
          double(:frame, register_temp_table: nil).tap do |csv_frame|
            loader = double(:result_loader)
            allow(loader).to receive(:load).with(path).and_return(csv_frame)
            formatted_reader = double(:reader)
            #noinspection RubyStringKeysInHashInspection
            java_options = read_options.to_java
            allow(formatted_reader).to receive(:options).with(java_options).and_return(loader)
            reader = double(:reader)
            allow(reader).to receive(:format).with('com.databricks.spark.csv').and_return(formatted_reader)
            allow_any_instance_of(Lib::CassandraSQLContext).to receive(:read) do |context|
              allow(csv_frame).to receive(:sql_context).and_return(context)
              reader
            end
          end
        end

        its(:record_klass) { is_expected.to eq(record_klass) }
        its(:spark_data_frame) { is_expected.to eq(csv_frame) }
        its(:table_name) { is_expected.to eq(table_name) }

        context 'with different csv options' do
          let(:options) { {infer_schema: 'true'} }
          let!(:csv_frame) { mock_csv_frame_load_variation(path, {'inferSchema' => 'true', 'header' => 'true'}) }
          let(:data_frame) { DataFrame.from_csv(record_klass, path, options) }

          its(:spark_data_frame) { is_expected.to eq(csv_frame) }

          context 'with an existing sql context' do
            let!(:csv_frame) { mock_csv_frame_load_variation(path) }
            let(:frame_context) { Lib::CassandraSQLContext.new(nil) }
            let(:options) { {sql_context: frame_context} }

            its(:sql_context) { is_expected.to eq(frame_context) }
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
        let(:spark_context) { global_application.spark_context }
        let(:keyspace) { Faker::Lorem.word }
        subject { data_frame.sql_context }
        before { record_klass.table.connection.config = {keyspace: keyspace} }

        it { is_expected.to eq(Lib::CassandraSQLContext.new(spark_context)) }

        it 'should cache the value' do
          data_frame.sql_context
          expect(Lib::CassandraSQLContext).not_to receive(:new)
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
        let(:sql_columns) { {'partition' => Lib::SqlStringType} }
        let(:sql_column_schema) { Lib::SqlDataFrame.create_schema(sql_columns) }
        subject { data_frame.spark_data_frame }

        it { is_expected.to be_a_kind_of(Lib::SqlDataFrame) }

        its(:rdd) { is_expected.to eq(Lib::SqlRowConversions.cassandraRDDToRowRDD(rdd)) }

        it 'should instance-cache the frame' do
          data_frame.spark_data_frame
          expect(Lib::SqlDataFrame).not_to receive(:new)
          data_frame.spark_data_frame
        end

        it 'should register a temp table with the name of the table associated with the frame' do
          expect_any_instance_of(Lib::SqlDataFrame).to receive(:register_temp_table).with(table_name)
          data_frame.spark_data_frame
        end

        context 'when a spark data frame is provided to the initializer' do
          let(:data_frame) { DataFrame.new(record_klass, nil, spark_data_frame: spark_frame, alias: spark_frame_alias) }

          it { is_expected.to eq(spark_frame) }
        end

        context 'with a specific table name specified' do
          let(:alias_table_name) { Faker::Lorem.word }
          let(:data_frame) { DataFrame.new(record_klass, rdd, alias: alias_table_name) }

          it 'should register a temp table with the alias' do
            expect_any_instance_of(Lib::SqlDataFrame).to receive(:register_temp_table).with(alias_table_name)
            data_frame.spark_data_frame
          end
        end

        its(:schema) { is_expected.to eq(sql_column_schema) }

        context 'with a different set of columns' do
          let(:clustering_columns) { {clustering: :int} }
          let(:sql_columns) { {'partition' => Lib::SqlStringType, 'clustering' => Lib::SqlIntegerType} }

          its(:schema) { is_expected.to eq(sql_column_schema) }
        end

        shared_examples_for 'mapping a cassandra column type to a spark sql type' do |cassandra_type, sql_type|
          let(:partition_key) { {partition: cassandra_type} }
          let(:sql_columns) { {'partition' => sql_type} }

          its(:schema) { is_expected.to eq(sql_column_schema) }
        end

        it_behaves_like 'mapping a cassandra column type to a spark sql type', :double, Lib::SqlDoubleType
        it_behaves_like 'mapping a cassandra column type to a spark sql type', :timestamp, Lib::SqlTimestampType
      end

      describe '#cache' do
        it 'should cache the data frame' do
          data_frame.cache
          expect(data_frame.spark_data_frame).to be_cached
        end
      end

      describe '#uncache' do
        it 'should unpersist the data frame' do
          data_frame.cache
          data_frame.uncache
          expect(data_frame.spark_data_frame).not_to be_cached
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

      describe '#first_async' do
        let(:record_result) { double(:result) }
        let(:params) { {Faker::Lorem.word => Faker::Lorem.sentence} }

        subject { data_frame.first_async(params) }

        before { allow(data_frame).to receive(:first).with(params).and_return(record_result) }

        it { is_expected.to be_a_kind_of(Cassandra::Future) }
        its(:get) { is_expected.to eq(record_result) }
      end

      describe '#normalized' do
        let(:partition_key) { available_columns.inject({}) { |memo, column| memo.merge!(column => :text) } }

        let(:available_columns) { [Faker::Lorem.word.to_sym] }
        let(:select_options) do
          available_columns.inject({}) { |memo, key| memo.merge!(key => {as: key}) }
        end
        let(:select_columns) { available_columns.map { |key| {key => {as: key}} } }
        let(:normalized_frame) { data_frame.select(select_options).as_data_frame(alias: :"normalized_#{table_name}") }
        let(:query_frame) { double(:data_frame, register_temp_table: nil, sql_context: data_frame.sql_context) }

        subject { data_frame.normalized }

        before do
          allow(data_frame).to receive(:query).with({}, select: select_columns).and_return(query_frame)
        end

        it { is_expected.to eq(normalized_frame) }

        context 'with multiple columns' do
          let(:available_columns) { 2.times.map { "#{Faker::Lorem.word}_#{Faker::Lorem.word}".to_sym } }

          it { is_expected.to eq(normalized_frame) }
        end

        context 'with a custom alias' do
          let(:alias_table_name) { Faker::Lorem.word }

          subject { data_frame.normalized(alias_table_name) }

          its(:table_name) { is_expected.to eq(alias_table_name) }
        end

        context 'with a row mapping' do
          let(:available_columns) { [:partition] }
          let(:select_columns) { [double_partition: {as: :double_partition}] }
          let(:select_options) { select_columns.first }
          let(:mapped_rdd) { double(:rdd) }
          let(:row_mapper) { double(:row_mapper, mappedRDD: mapped_rdd) }
          let(:rdd_mapper) do
            {
                mapper: row_mapper,
                type_map: {
                    partition: {type: Lib::SqlDoubleType, name: :double_partition}
                }
            }
          end
          let(:record_klass_rdd_mapper) { rdd_mapper }

          it { is_expected.to eq(normalized_frame) }
        end

        context 'when created from another DataFrame' do
          let(:duplicate_frame) { data_frame.select(:partition).as_data_frame(alias: :new_frame) }

          subject { duplicate_frame.normalized }

          before do
            allow(data_frame).to receive(:query).with({}, select: [:partition]).and_return(query_frame)
          end

          it { is_expected.to eq(duplicate_frame) }
        end
      end

      describe '#sql' do
        let(:select_key) { Faker::Lorem.word }
        let(:result_sql_type) { Lib::SqlStringType }
        let(:result) { {select_key.to_sym => Faker::Lorem.word} }
        let(:fields) { [Lib::SqlStructField.new(select_key, result_sql_type, true, Lib::SqlMetadata.empty)] }
        let(:query_schema) { Lib::SqlStructType.new(fields) }
        let(:query_result) { double(:query, collect: [Lib::RDDRow[result]], schema: query_schema) }
        let(:query) { "SELECT * FROM #{table_name}" }

        subject { data_frame.sql(query) }

        before do
          allow(data_frame.sql_context).to receive(:sql).with(query).and_return(query_result)
        end

        it { is_expected.to eq([result]) }

        it 'should ensure that the data_frame has been created' do
          expect(data_frame).to receive(:spark_data_frame)
          subject
        end
      end

      describe '#sql_frame' do
        let(:new_frame) { double(:frame) }
        let(:query_result) { double(:query, register_temp_table: nil, sql_context: data_frame.sql_context) }
        let(:query) { "SELECT * FROM #{table_name}" }
        let(:new_class) { double(:class, table_name: nil, rdd_row_mapping: nil) }
        let(:alias_table_name) { Faker::Lorem.word.to_sym }
        let(:derived_frame) { DataFrame.new(new_class, nil, spark_data_frame: query_result, alias: alias_table_name) }

        subject { data_frame.sql_frame(query, class: new_class, alias: alias_table_name) }

        before { allow(data_frame.sql_context).to receive(:sql).with(query).and_return(query_result) }

        it { is_expected.to eq(derived_frame) }

        context 'without a record klass provided' do
          let(:new_class) { nil }
          let(:derived_frame) { DataFrame.new(data_frame.record_klass, nil, spark_data_frame: query_result, alias: alias_table_name) }

          it { is_expected.to eq(derived_frame) }
        end

        it 'should ensure that the data_frame has been created' do
          expect(data_frame).to receive(:spark_data_frame)
          subject
        end
      end

      describe '#query' do
        let(:sql_context) { Lib::CassandraSQLContext.new(nil) }
        let(:query) { double(:query) }
        let(:restriction) { {} }
        let(:options) { {} }
        let(:query_sql) { "SELECT * FROM #{table_name}" }
        let(:data_frame) { DataFrame.new(record_klass, rdd, sql_context: sql_context) }
        let(:mock_schema) { Lib::SqlDataFrame.create_schema(rk_partition: :string, ck_partition: :string, ck_price: :double, partition: :string, price: :double) }

        subject { data_frame.query(restriction, options) }

        before do
          allow(data_frame.spark_data_frame).to receive(:schema).and_return(mock_schema)
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
            let(:clustering_columns) { {price: :double} }
            let(:restriction) { {:price.gt => 50.49} }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `price` > 50.49" }

            it { is_expected.to eq(query) }

            context 'when the columns are mapped' do
              let(:query_sql) { "SELECT * FROM #{table_name} WHERE `ck_price` > 50.49" }
              let(:record_klass) { composite_record_klass }

              it { is_expected.to eq(query) }
            end

            context 'with a ColumnCast' do
              let(:restriction) { {:price.cast_as(:double).gt => 50.49} }
              let(:query_sql) { "SELECT * FROM #{table_name} WHERE CAST(`price` AS DOUBLE) > 50.49" }

              it { is_expected.to eq(query) }
            end

            context 'when provided with a child column' do
              let(:child_key) { Faker::Lorem.word.to_sym }
              let(:restriction) { {:price.child(child_key).lt => 43.99} }
              let(:query_sql) { "SELECT * FROM #{table_name} WHERE `price`.`#{child_key}` < 43.99" }

              it { is_expected.to eq(query) }

              context 'when the columns are mapped' do
                let(:query_sql) { "SELECT * FROM #{table_name} WHERE `ck_price`.`#{child_key}` < 43.99" }
                let(:record_klass) { composite_record_klass }

                it { is_expected.to eq(query) }
              end
            end
          end

          context 'when the columns are mapped' do
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `rk_partition` = 47" }
            let(:record_klass) { composite_record_klass }

            it { is_expected.to eq(query) }
          end

          context 'when provided with a child column' do
            let(:child_key) { Faker::Lorem.word.to_sym }
            let(:restriction) { {:partition.child(child_key) => 49.99} }
            let(:query_sql) { "SELECT * FROM #{table_name} WHERE `partition`.`#{child_key}` = 49.99" }

            it { is_expected.to eq(query) }

            context 'when the columns are mapped' do
              let(:query_sql) { "SELECT * FROM #{table_name} WHERE `rk_partition`.`#{child_key}` = 49.99" }
              let(:record_klass) { composite_record_klass }

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
            let(:record_klass) { composite_record_klass }

            it { is_expected.to eq(query) }
          end

          context 'with a casted column' do
            let(:options) { {method => [:partition.cast_as(:int)]} }
            let(:query_sql) { "SELECT * FROM #{table_name} #{clause} CAST(`partition` AS INT)" }

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

          context 'when selecting a constant' do
            let(:options) { {select: [4]} }
            let(:query_sql) { "SELECT 4 FROM #{table_name}" }

            it { is_expected.to eq(query) }

            context 'when that constant is a string' do
              let(:select_constant) { Faker::Lorem.word }
              let(:options) { {select: [select_constant]} }
              let(:query_sql) { "SELECT '#{select_constant}' FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            context 'when the constant contains quotes' do
              let(:select_constant) { "'; DROP TABLE some_table; SELECT '" }
              let(:options) { {select: [select_constant]} }
              let(:query_sql) { "SELECT '\\'; DROP TABLE some_table; SELECT \\'' FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end
          end

          context 'with multiple columns' do
            let(:options) { {select: [:partition, :clustering]} }
            let(:query_sql) { "SELECT `partition`, `clustering` FROM #{table_name}" }

            it { is_expected.to eq(query) }
          end

          context 'when the columns are mapped' do
            let(:query_sql) { "SELECT `rk_partition` FROM #{table_name}" }
            let(:record_klass) { composite_record_klass }

            it { is_expected.to eq(query) }

            context 'when the mapped column is not part of the DataFrame schema' do
              let(:query_sql) { "SELECT `partition` FROM #{table_name}" }
              let(:mock_schema) { Lib::SqlDataFrame.create_schema('partition' => 'string') }

              it { is_expected.to eq(query) }
            end

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

              context 'with multiple column in the aggregate' do
                let(:options) { {select: [{[:partition, :clustering] => {aggregate: aggregate}}]} }
                let(:query_sql) { "SELECT #{sql_aggregate}(`partition`, `clustering`) FROM #{table_name}" }

                it { is_expected.to eq(query) }
              end
            end

            it_behaves_like 'an aggregate function', :count
            it_behaves_like 'an aggregate function', :sum

            shared_examples_for 'casting as column to another type' do |type|
              let(:aggregate) { :"cast_#{type}" }
              let(:query_sql) { "SELECT CAST(`partition` AS #{type.to_s.upcase}) FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

            it_behaves_like 'casting as column to another type', :int
            it_behaves_like 'casting as column to another type', :double
            it_behaves_like 'casting as column to another type', :string

            context 'when provided with a ColumnCast' do
              let(:aggregate) { :max }
              let(:options) { {select: [{:partition.cast_as(:int) => {aggregate: aggregate}}]} }
              let(:query_sql) { "SELECT MAX(CAST(`partition` AS INT)) FROM #{table_name}" }

              it { is_expected.to eq(query) }
            end

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
      end

      describe 'pulling data from the data set' do
        shared_examples_for 'a method mapping a query result to a Record' do |method, collect_method|
          let(:result_sql_type) { Lib::SqlTypeWrapper.new(Lib::SqlStringType) }
          let(:result_value) { Faker::Lorem.word }

          let(:clustering_columns) { available_columns.inject({}) { |memo, column| memo.merge!(column => :text) } }

          let(:key) { Faker::Lorem.word }
          let(:attributes) { {key => Faker::Lorem.word} }
          let(:result) { {select_key => result_value} }
          let(:select_key) { Faker::Lorem.word }
          let(:options) { {select: [select_key]} }
          let(:available_columns) { [select_key.to_sym] }

          let(:fields) { [Lib::SqlStructField.new(select_key, result_sql_type, true, Lib::SqlMetadata.empty)] }
          let(:query_schema) { Lib::SqlStructType.new(fields) }
          let(:query) { double(:query, schema: query_schema, first: Lib::RDDRow[result], collect: [Lib::RDDRow[result]]) }
          let(:record_attributes) { {select_key.to_sym => result_value} }

          before do
            allow(data_frame).to receive(:query).with(attributes, options).and_return(query)
          end

          it 'should support default values' do
            allow(data_frame).to receive(:query).and_return(query)
            expect { data_frame.public_send(method) }.not_to raise_error
          end

          it 'should return the result mapped to a CassandraModel::Record' do
            expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
          end

          shared_examples_for 'converting sql types back to ruby types' do |value, sql_type|
            let(:result_sql_type) { Lib::SqlTypeWrapper.new(sql_type) }
            let(:result_value) { value }

            it 'should return the result mapped to a CassandraModel::Record' do
              expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
            end
          end

          it_behaves_like 'converting sql types back to ruby types', 15, Lib::SqlIntegerType
          it_behaves_like 'converting sql types back to ruby types', Lib::SqlLong.new(153), Lib::SqlLongType
          it_behaves_like 'converting sql types back to ruby types', 15.3, Lib::SqlDoubleType
          it_behaves_like 'converting sql types back to ruby types', Time.at(12544), Lib::SqlTimestampType
          it_behaves_like 'converting sql types back to ruby types', {'hello' => 'world'}, Lib::SqlMapType.apply(Lib::SqlStringType, Lib::SqlStringType, true)

          describe 'converting uuid types' do
            let(:clustering_columns) { {select_key.to_sym => :uuid} }
            it_behaves_like 'converting sql types back to ruby types', Cassandra::Uuid.new('00000000-0000-0000-0000-000000000001'), Lib::SqlStringType

            context 'when the columns are mapped' do
              let(:mapped_column) { :"ck_#{select_key}" }
              let(:clustering_columns) { {select_key => :uuid} }
              let(:record_klass) { composite_record_klass }
              it_behaves_like 'converting sql types back to ruby types', Cassandra::Uuid.new('00000000-0000-0000-0000-000000000001'), Lib::SqlStringType
            end
          end

          describe 'converting timeuuid types' do
            let(:clustering_columns) { {select_key.to_sym => :timeuuid} }
            it_behaves_like 'converting sql types back to ruby types', Cassandra::TimeUuid.new('00000000-0000-0000-0000-000000000011'), Lib::SqlStringType

            context 'when the columns are mapped' do
              let(:mapped_column) { :"ck_#{select_key}" }
              let(:clustering_columns) { {select_key => :timeuuid} }
              let(:record_klass) { composite_record_klass }
              it_behaves_like 'converting sql types back to ruby types', Cassandra::TimeUuid.new('00000000-0000-0000-0000-000000000011'), Lib::SqlStringType
            end
          end

          context 'when a type is a StructType' do
            let(:sql_type) { Lib::SqlStructType.new([Lib::SqlStructField.new('description', Lib::SqlStringType, true, Lib::SqlMetadata.empty)]) }
            let(:result_sql_type) { sql_type }
            let(:result_value) { Lib::RDDRow[description: Faker::Lorem.word] }

            it 'should recursively map the results' do
              expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
            end
          end

          context 'with a type we cannot handle' do
            let(:result_sql_type) { Lib::SqlTypeWrapper.new(Lib::SqlFakeType) }
            let(:result_value) { '1239333-33333' }

            it 'should convert to a string' do
              expect(data_frame.public_send(method, attributes, options)).to eq(record_result)
            end
          end

          context 'when the record maps result columns' do
            let(:result) { {"ck_#{select_key}" => result_value} }
            let(:fields) { [Lib::SqlStructField.new("ck_#{select_key}", result_sql_type, true, Lib::SqlMetadata.empty)] }
            let(:record_klass) { composite_record_klass }

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

        describe '#to_csv' do
          let(:path) { "#{Faker::Internet.url}" }
          let!(:csv_saver) { mock_csv_frame_save_variation }

          def mock_csv_frame_save_variation(write_options = {'header' => 'true'})
            double(:result_saver).tap do |saver|
              formatted_writer = double(:writer)
              #noinspection RubyStringKeysInHashInspection
              java_options = write_options.to_java
              allow(formatted_writer).to receive(:options).with(java_options).and_return(saver)
              writer = double(:writer)
              allow(writer).to receive(:format).with('com.databricks.spark.csv').and_return(formatted_writer)
              allow(data_frame.spark_data_frame).to receive(:write).and_return(writer)
            end
          end

          it 'should save the DataFrame to the specified path as a csv' do
            expect(csv_saver).to receive(:save).with(path)
            data_frame.to_csv(path)
          end

          context 'with different csv options' do
            let(:options) { {null_value: '|'} }
            let!(:csv_saver) { mock_csv_frame_save_variation('nullValue' => '|', 'header' => 'true') }

            it 'should save the DataFrame to the specified path as a csv using the specified options' do
              expect(csv_saver).to receive(:save).with(path)
              data_frame.to_csv(path, options)
            end
          end

        end

        describe '#save_to' do
          let(:save_table_name) { Faker::Lorem.word }
          let(:save_keyspace) { Faker::Lorem.word }
          let(:save_connection_config) { {keyspace: save_keyspace} }
          let(:save_connection) { double(:raw_connection, config: save_connection_config) }
          let(:save_table) { double(:table, connection: save_connection) }
          let(:save_column_map) { symbolized_field_names.inject({}) { |memo, column| memo.merge!(column => column) } }
          let(:composite_defaults) { [] }
          let(:save_record_klass) do
            double(:klass, table: save_table, table_name: save_table_name, composite_defaults: composite_defaults)
          end
          let(:field_names) { Faker::Lorem.words }
          let(:symbolized_field_names) { field_names.map(&:to_sym) }
          let(:fields) { field_names.map { |name| Lib::SqlStructField.new(name, nil, true, Lib::SqlMetadata.empty) } }
          let(:schema) { Lib::SqlStructType.new(fields) }

          before do
            allow(data_frame.spark_data_frame).to receive(:schema).and_return(schema)
            allow(save_record_klass).to receive(:denormalized_column_map).with(symbolized_field_names).and_return(save_column_map)
          end

          def mock_frame_save_variation(query_options = nil)
            double(:result_saver, save: nil).tap do |saver|
              cassandra_writer = double(:writer)
              allow(cassandra_writer).to receive(:mode).with('Append').and_return(saver)
              formatted_writer = double(:writer)
              #noinspection RubyStringKeysInHashInspection
              java_options = {'table' => save_table_name, 'keyspace' => save_keyspace}.to_java
              allow(formatted_writer).to receive(:options).with(java_options).and_return(cassandra_writer)
              writer = double(:writer)
              allow(writer).to receive(:format).with('org.apache.spark.sql.cassandra').and_return(formatted_writer)
              if query_options
                frame = double(:frame, write: writer)
                allow(data_frame).to receive(:query).with({}, query_options).and_return(frame)
              else
                allow(data_frame.spark_data_frame).to receive(:write).and_return(writer)
              end
            end
          end

          describe 'a simple one-to-one save' do
            let(:saver) { mock_frame_save_variation }

            it 'should save using the spark data frame' do
              expect(saver).to receive(:save)
              data_frame.save_to(save_record_klass)
            end
          end

          context 'when the columns need mapping' do
            let(:select_clause) do
              save_column_map.map do |target, source|
                {source => {as: target}}
              end
            end
            let(:save_column_map) do
              field_names.map(&:to_sym).inject({}) do |memo, column|
                memo.merge!(:"rk_#{column}" => column)
              end
            end
            let!(:saver) { mock_frame_save_variation(select: select_clause) }

            it 'should save using the mapped columns from a sql query' do
              expect(saver).to receive(:save)
              data_frame.save_to(save_record_klass)
            end

            context 'when the target model provides a truth table' do
              let(:composite_defaults) { [{rk_pk3: ''}, {rk_pk1: 0, rk_pk2: -1}] }
              let(:field_names) { %w(pk1 pk2 pk3) }
              let(:select_clause_two) do
                [{pk1: {as: :rk_pk1}}, {pk2: {as: :rk_pk2}}, {'' => {as: :rk_pk3}}]
              end
              let!(:saver_two) { mock_frame_save_variation(select: select_clause_two) }
              let(:select_clause_three) do
                [{0 => {as: :rk_pk1}}, {-1 => {as: :rk_pk2}}, {pk3: {as: :rk_pk3}}]
              end
              let!(:saver_three) { mock_frame_save_variation(select: select_clause_three) }

              it 'should save the truth table values for the first set of defaults' do
                expect(saver_two).to receive(:save)
                data_frame.save_to(save_record_klass)
              end

              it 'should save the truth table values for the second set of defaults' do
                expect(saver_three).to receive(:save)
                data_frame.save_to(save_record_klass)
              end

              describe 'defaulting uuid columns' do
                let(:default_uuid) { SecureRandom.uuid }
                let(:composite_defaults) { [{rk_pk3: uuid_klass.new(default_uuid)}] }
                let(:select_clause_two) do
                  [{pk1: {as: :rk_pk1}}, {pk2: {as: :rk_pk2}}, {default_uuid => {as: :rk_pk3}}]
                end
                let!(:saver_three) { nil }

                describe 'Cassandra::Uuid' do
                  let(:uuid_klass) { Cassandra::Uuid }

                  it 'should convert the uuid to a string before saving' do
                    expect(saver_two).to receive(:save)
                    data_frame.save_to(save_record_klass)
                  end
                end

                describe 'Cassandra::TimeUuid' do
                  let(:uuid_klass) { Cassandra::TimeUuid }

                  it 'should convert the uuid to a string before saving' do
                    expect(saver_two).to receive(:save)
                    data_frame.save_to(save_record_klass)
                  end
                end
              end
            end
          end
        end

        describe '#first' do
          let(:record_result) { record_klass.new(record_attributes) }
          it_behaves_like 'a method mapping a query result to a Record', :first, :first
        end

        describe '#request' do
          let(:record_result) { [record_klass.new(record_attributes)] }
          it_behaves_like 'a method mapping a query result to a Record', :request, :collect
        end

      end
    end
  end
end
