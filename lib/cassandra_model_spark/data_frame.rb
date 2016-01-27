module CassandraModel
  module Spark
    #noinspection RubyStringKeysInHashInspection
    class DataFrame
      include QueryHelper

      SQL_TYPE_MAP = {
          int: SqlIntegerType,
          text: SqlStringType,
          double: SqlDoubleType,
          timestamp: SqlTimestampType,
      }.freeze
      #noinspection RubyStringKeysInHashInspection
      SQL_RUBY_TYPE_FUNCTIONS = {
          'IntegerType' => :getInt,
          'LongType' => :getLong,
          'StringType' => :getString,
          'DoubleType' => :getDouble,
          'TimestampType' => :getTimestamp,
          'MapType(StringType,StringType,true)' => :getMap,
      }

      attr_reader :table_name, :record_klass

      class << self
        def from_csv(record_klass, path, options = {})
          sql_context = options.delete(:sql_context) || create_sql_context(record_klass)
          updated_options = csv_options(options)
          csv_frame = sql_context.read.format('com.databricks.spark.csv').options(updated_options).load(path)

          table_name = File.basename(path).gsub(/\./, '_') + "_#{SecureRandom.hex(2)}"
          new(record_klass, nil, spark_data_frame: csv_frame, alias: table_name)
        end

        def create_sql_context(record_klass)
          CassandraSQLContext.new(record_klass.table.connection.spark_context).tap do |context|
            context.setKeyspace(record_klass.table.connection.config[:keyspace])
          end
        end

        def csv_options(options)
          options.inject('header' => 'true') do |memo, (key, value)|
            memo.merge!(key.to_s.camelize(:lower) => value)
          end.to_java
        end
      end

      def initialize(record_klass, rdd, options = {})
        @table_name = options.fetch(:alias) { record_klass.table_name }
        @sql_context = options[:sql_context]
        initialize_frame_from_existing(options)
        @record_klass = record_klass

        initialize_row_mapping(options)
        initialize_rdd(rdd)
      end

      def derived?
        !!@derived
      end

      def sql_context
        @sql_context ||= self.class.create_sql_context(record_klass)
      end

      def union(rhs)
        unless record_klass == rhs.record_klass
          raise ArgumentError, 'Cannot union DataFrames with different Record types!'
        end
        DataFrame.new(record_klass, rdd.union(rhs.rdd))
      end

      def spark_data_frame
        @frame ||= SparkSchemaBuilder.new.tap do |builder|
          record_klass.cassandra_columns.each do |name, type|
            select_name = record_klass.normalized_column(name)
            mapped_type = row_type_mapping[select_name]
            type = if mapped_type
                     name = mapped_type[:name]
                     mapped_type[:type]
                   else
                     SQL_TYPE_MAP.fetch(type) { SqlStringType }
                   end
            builder.add_column(name.to_s, type)
          end
        end.create_data_frame(sql_context, rdd).tap { |frame| frame.register_temp_table(table_name.to_s) }
      end

      def cache
        spark_data_frame.cache
      end

      def uncache
        spark_data_frame.unpersist
      end

      def cached(&block)
        spark_data_frame.cache
        instance_eval(&block)
        spark_data_frame.unpersist
      end

      def normalized(alias_table_name = nil)
        return self unless rdd

        select_options = record_klass.columns.inject({}) do |memo, column|
          row_mapped_column = row_type_mapping.fetch(column) { {name: column} }[:name]
          memo.merge!(row_mapped_column => {as: row_mapped_column})
        end
        alias_name = alias_table_name || :"normalized_#{table_name}"
        select(select_options).as_data_frame(alias: alias_name)
      end

      def request_async(*_)
        ResultPaginator.new(first_async) {}
      end

      def first_async(*_)
        Cassandra::Future.error(NotImplementedError.new)
      end

      def sql(query)
        spark_data_frame
        query = sql_context.sql(query)
        query.collect.map do |row|
          row_to_record(query.schema, row)
        end

      end

      def sql_frame(query, options)
        spark_data_frame
        new_frame = sql_context.sql(query)
        self.class.new(options.delete(:class) || record_klass, nil, options.merge(spark_data_frame: new_frame))
      end

      def query(restriction, options)
        spark_data_frame
        select_clause = select_columns(options)
        group_clause = group_clause(:group, 'GROUP BY', options)
        order_clause = group_clause(:order_by, 'ORDER BY', options)
        limit_clause = if options[:limit]
                         " LIMIT #{options[:limit]}"
                       end
        where_clause = query_where_clause(restriction)
        sql_context.sql("SELECT #{select_clause} FROM #{table_name}#{where_clause}#{group_clause}#{order_clause}#{limit_clause}")
      end

      def request(restriction = {}, options = {})
        query = query(restriction, options)
        query.collect.map do |row|
          row_to_record(query.schema, row)
        end
      end

      def first(restriction = {}, options = {})
        query = query(restriction, options)
        row = query.first
        row_to_record(query.schema, row)
      end

      def to_csv(path, options = {})
        updated_options = csv_options(options)
        spark_data_frame.write.format('com.databricks.spark.csv').options(updated_options).save(path)
      end

      def save_to(save_record_klass)
        #noinspection RubyStringKeysInHashInspection
        java_options = save_options_for_model(save_record_klass)

        available_columns = spark_data_frame.schema.fields.map(&:name).map(&:to_sym)
        column_map = save_record_klass.denormalized_column_map(available_columns)

        save_frame = frame_to_save(available_columns, column_map)
        save_frame(java_options, save_frame)
        save_truth_table(column_map, java_options, save_record_klass)
      end

      def ==(rhs)
        rhs.is_a?(DataFrame) &&
            record_klass == rhs.record_klass &&
            ((rdd && rdd == rhs.rdd) || (!rdd && spark_data_frame == rhs.spark_data_frame))
      end

      protected

      attr_reader :rdd

      private

      def initialize_frame_from_existing(options)
        @frame = options[:spark_data_frame]
        if @frame
          raise ArgumentError, 'DataFrames created from Spark DataFrames require aliases!' unless options[:alias]
          @frame.register_temp_table(options[:alias].to_s)
          @sql_context = @frame.sql_context
        end
      end

      def initialize_rdd(rdd)
        if rdd
          @rdd = if @row_mapping[:mapper]
                   @row_mapping[:mapper].mappedRDD(rdd)
                 else
                   rdd
                 end
        else
          @derived = true
        end
      end

      def initialize_row_mapping(options)
        @row_mapping = options.fetch(:row_mapping) do
          @record_klass.rdd_row_mapping || {}
        end
      end

      def row_type_mapping
        @row_mapping[:type_map] ||= {}
      end

      def row_to_record(schema, row)
        attributes = row_attributes(row, schema)

        if valid_record?(attributes)
          record_klass.new(attributes)
        else
          attributes
        end
      end

      def row_attributes(row, schema)
        attributes = {}
        schema.fields.each_with_index do |field, index|
          value = field_value(field, index, row)
          column = field.name
          attributes.merge!(column => value)
        end
        record_klass.normalized_attributes(attributes)
      end

      def valid_record?(attributes)
        available_columns = record_klass.columns + record_klass.deferred_columns
        attributes.keys.all? { |column| available_columns.include?(column) }
      end

      def field_value(field, index, row)
        data_type = field.data_type
        if column_is_struct?(data_type)
          row_attributes(row.get(index), data_type)
        else
          decode_column_value(field, index, row)
        end
      end

      def decode_column_value(field, index, row)
        sql_type = field.data_type.to_string
        converter = SQL_RUBY_TYPE_FUNCTIONS.fetch(sql_type) { :getString }
        value = row.public_send(converter, index)

        data_column_name = record_klass.select_column(field.name.to_sym)
        case record_klass.cassandra_columns[data_column_name]
          when :uuid
            value = Cassandra::Uuid.new(value)
          when :timeuuid
            value = Cassandra::TimeUuid.new(value)
        end

        value = decode_hash(value) if column_is_string_map?(sql_type)
        value
      end

      def decode_hash(value)
        Hash[value.toSeq.array.to_a.map! { |pair| [pair._1.to_string, pair._2.to_string] }]
      end

      def column_is_string_map?(sql_type)
        sql_type == 'MapType(StringType,StringType,true)'
      end

      def column_is_struct?(data_type)
        data_type.getClass.getSimpleName == 'StructType'
      end

      def select_columns(options)
        options[:select] ? clean_select_columns(options) * ', ' : '*'
      end

      def group_clause(type, prefix, options)
        if options[type]
          updated_clause = options[type].map do |column|
            if column.is_a?(Hash)
              column, direction = column.first
              updated_column = quoted_column(column)
              "#{updated_column} #{direction.upcase}"
            else
              quoted_column(column)
            end
          end * ', '
          " #{prefix} #{updated_clause}"
        end
      end

      def group_child_clause(child, updated_column)
        child, direction = if child.is_a?(Hash)
                             child.first
                           else
                             [child]
                           end
        direction_clause = (" #{direction.upcase}" if direction)
        "#{updated_column}.`#{child}`#{direction_clause}"
      end

      def clean_select_columns(options)
        options[:select].map do |column|
          if column.is_a?(Hash)
            updated_column(column)
          else
            quoted_column(column)
          end
        end
      end

      def updated_column(column)
        column, options = column.first

        if options.is_a?(Symbol)
          updated_column = if column.is_a?(ThomasUtils::KeyChild)
                             "#{column}".gsub(/\./, '_')
                           else
                             column
                           end
          options = {aggregate: options, as: :"#{updated_column}_#{options}"}
        end

        column = quoted_column(column)
        column = aggregate_column(column, options) if options[:aggregate]
        column = "#{column} AS #{options[:as]}" if options[:as]
        column
      end

      def quoted_column(column)
        return column.map { |child_column| quoted_column(child_column) } * ', ' if column.is_a?(Array)

        if column == :*
          '*'
        elsif column.respond_to?(:quote)
          column.quote('`')
        elsif column.is_a?(Symbol)
          "`#{select_column(column)}`"
        elsif column.is_a?(String)
          "'#{column.gsub(/'/, "\\\\'")}'"
        else
          column
        end
      end

      def aggregate_column(column, options)
        case options[:aggregate]
          when :count_distinct
            "COUNT(#{distinct_aggregate(column)})"
          when :distinct
            distinct_aggregate(column)
          when :variance
            variance_column(column)
          when :stddev
            "POW(#{variance_column(column)},0.5)"
          else
            if options[:aggregate] =~ /^cast_/
              type = options[:aggregate].to_s.match(/^cast_(.+)$/)[1]
              "CAST(#{column} AS #{type.upcase})"
            else
              "#{options[:aggregate].to_s.upcase}(#{column})"
            end
        end
      end

      def distinct_aggregate(column)
        "DISTINCT #{column}"
      end

      def variance_column(column)
        "AVG(POW(#{column},2)) - POW(AVG(#{column}),2)"
      end

      def query_where_clause(restriction)
        if restriction.present?
          restriction_clause = restriction.map do |key, value|
            updated_key = if key.is_a?(ThomasUtils::KeyComparer)
                            select_key = if key.key.respond_to?(:new_key)
                                           select_key = select_column(key.key.key)
                                           key.key.new_key(select_key)
                                         else
                                           select_column(key.key)
                                         end
                            key.new_key(select_key).quote('`')
                          elsif key.is_a?(ThomasUtils::KeyChild)
                            new_key = select_column(key.key)
                            updated_key = key.new_key(new_key)
                            quoted_restriction(updated_key)
                          else
                            select_key = select_column(key)
                            quoted_restriction(select_key)
                          end
            value = "'#{value}'" if value.is_a?(String) || value.is_a?(Time)
            "#{updated_key} #{value}"
          end * ' AND '
          " WHERE #{restriction_clause}"
        end
      end

      def select_column(key)
        new_key = record_klass.select_column(key)
        available_columns.include?(new_key) ? new_key : key
      end

      def available_columns
        @available_columns ||= spark_data_frame.schema.fields.map(&:name).map(&:to_sym)
      end

      def quoted_restriction(updated_key)
        ThomasUtils::KeyComparer.new(updated_key, '=').quote('`')
      end

      def frame_to_save(available_columns, column_map)
        if available_columns == column_map.keys
          spark_data_frame
        else
          select_clause = save_select_clause(column_map)
          query({}, select: select_clause)
        end
      end

      def csv_options(options)
        self.class.csv_options(options)
      end

      def save_options_for_model(save_record_klass)
        {
            'table' => save_record_klass.table_name,
            'keyspace' => save_record_klass.table.connection.config[:keyspace]
        }.to_java
      end

      def save_truth_table(column_map, java_options, save_record_klass)
        save_record_klass.composite_defaults.each do |row|
          select_clause = save_select_clause(column_map.merge(row))
          frame = query({}, select: select_clause)
          save_frame(java_options, frame)
        end
      end

      def save_frame(java_options, save_frame)
        save_frame.write.format('org.apache.spark.sql.cassandra').options(java_options).mode('Append').save
      end

      def save_select_clause(updated_column_map)
        updated_column_map.map do |target, source|
          {source => {as: target}}
        end
      end

    end
  end
end
