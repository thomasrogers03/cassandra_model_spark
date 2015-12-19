module CassandraModel
  module Spark
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
      }

      attr_reader :table_name, :record_klass

      def initialize(record_klass, rdd, options = {})
        @table_name = options.fetch(:alias) { record_klass.table_name }
        @sql_context = options[:sql_context]
        initialize_frame_from_existing(options)
        @record_klass = record_klass

        initialize_row_mapping(options)
        initialize_rdd(rdd)
      end

      def sql_context
        @sql_context ||= create_sql_context
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
            new_name = mapped_type ? mapped_type[:name] : name
            type = if mapped_type
                     mapped_type[:type]
                   else
                     SQL_TYPE_MAP.fetch(type) { SqlStringType }
                   end
            builder.add_column(new_name.to_s, type)
          end
        end.create_data_frame(sql_context, rdd).tap { |frame| frame.register_temp_table(table_name.to_s) }
      end

      def cached(&block)
        spark_data_frame.cache
        instance_eval(&block)
        spark_data_frame.unpersist
      end

      def request_async(*_)
        ResultPaginator.new(first_async) {}
      end

      def first_async(*_)
        Cassandra::Future.error(NotImplementedError.new)
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
          row_to_record(query, row)
        end
      end

      def first(restriction = {}, options = {})
        query = query(restriction, options)
        row = query.first
        row_to_record(query, row)
      end

      def ==(rhs)
        rhs.is_a?(DataFrame) &&
            record_klass == rhs.record_klass &&
            rdd == rhs.rdd
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
        @rdd = if @row_mapping[:mapper]
                 @row_mapping[:mapper].mappedRDD(rdd)
               else
                 rdd
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

      def create_sql_context
        CassandraSQLContext.new(record_klass.table.connection.spark_context).tap do |context|
          context.setKeyspace(record_klass.table.connection.config[:keyspace])
        end
      end

      def row_to_record(query, row)
        attributes = query.schema.fields.each.with_index.inject({}) do |memo, (field, index)|
          converter = SQL_RUBY_TYPE_FUNCTIONS.fetch(field.data_type.to_string) { :getString }
          value = row.public_send(converter, index)
          column = field.name
          memo.merge!(column => value)
        end
        attributes = record_klass.normalized_attributes(attributes)
        if attributes.keys.all? { |column| record_klass.columns.include?(column) }
          record_klass.new(attributes)
        else
          attributes
        end
      end

      def select_columns(options)
        options[:select] ? clean_select_columns(options) * ', ' : '*'
      end

      def group_clause(type, prefix, options)
        if options[type]
          updated_clause = options[type].map do |column|
            if column.is_a?(Hash)
              column, direction = column.first
              "#{quoted_column(column)} #{direction.upcase}"
            else
              quoted_column(column)
            end
          end * ', '
          " #{prefix} #{updated_clause}"
        end
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
          options = {aggregate: options, as: :"#{column}_#{options}"}
        end

        column = quoted_column(column)
        column = aggregate_column(column, options) if options[:aggregate]
        column = "#{column} AS #{options[:as]}" if options[:as]
        column
      end

      def quoted_column(column)
        if column == :*
          '*'
        else
          "`#{record_klass.select_column(column)}`"
        end
      end

      def aggregate_column(column, options)
        case options[:aggregate]
          when :count_distinct
            "COUNT(DISTINCT(#{column}))"
          when :variance
            variance_column(column)
          when :stddev
            "POW(#{variance_column(column)},0.5)"
          else
            "#{options[:aggregate].to_s.upcase}(#{column})"
        end
      end

      def variance_column(column)
        "AVG(POW(#{column},2)) - POW(AVG(#{column}),2)"
      end

      def query_where_clause(restriction)
        if restriction.present?
          restriction_clause = restriction.map do |key, value|
            updated_key = if key.is_a?(ThomasUtils::KeyComparer)
                            select_key = record_klass.select_column(key.key)
                            key.new_key(select_key)
                          else
                            select_key = record_klass.select_column(key)
                            "#{select_key} ="
                          end
            value = "'#{value}'" if value.is_a?(String) || value.is_a?(Time)
            "#{updated_key} #{value}"
          end * ' AND '
          " WHERE #{restriction_clause}"
        end
      end
    end
  end
end
