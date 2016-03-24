module VariousRecords
  extend RSpec::SharedContext

  let(:extra_basic_record_klass) do
    create_query = 'CREATE TABLE IF NOT EXISTS super_basic (key text, value text, description text, extra_data blob, PRIMARY KEY (key, value))'
    CassandraModel::ConnectionCache[nil].session.execute(create_query)
    Class.new(CassandraModel::Record) do
      self.table_name = :super_basic
      table.allow_truncation!
      table.truncate!
    end
  end
  let(:basic_record_klass) do
    Class.new(CassandraModel::Record) do
      name = :basic
      partition_key = {key: :text}
      clustering_columns = {value: :text}
      fields = {description: :text, extra_data: :blob}
      table_definition = CassandraModel::TableDefinition.new(
          name: name,
          partition_key: partition_key,
          clustering_columns: clustering_columns,
          remaining_columns: fields
      )
      self.table = CassandraModel::MetaTable.new(table_definition)
      table.allow_truncation!
      table.truncate!
    end
  end
  let(:composite_record_klass) do
    Class.new(CassandraModel::Record) do
      extend CassandraModel::DataModelling
      self.table_name = :composite
      model_data do |inquirer, data_set|
        inquirer.knows_about(:key)
        data_set.is_defined_by(:value)
        data_set.knows_about(:description, :extra_data)
        data_set.change_type_of(:extra_data).to(:blob)
      end
      table.allow_truncation!
      table.truncate!
    end
  end

  def generate_simple_model(name, partition, clustering, fields, rdd_row_mapping = nil, deferred_columns = [], &block)
    column_types = (partition.merge(clustering).merge(fields)).map { |column, type| "#{column} #{type}" } * ', '
    partition = "(#{partition.keys * ', '})"
    clustering = "#{clustering.keys * ', '}"
    create_query = "CREATE TABLE IF NOT EXISTS #{name} (#{column_types}, PRIMARY KEY (#{partition}, #{clustering}))"
    CassandraModel::ConnectionCache[nil].session.execute(create_query)
    Class.new(CassandraModel::Record) do
      self.table_name = name
      table.allow_truncation!
      table.truncate!
      @rdd_row_mapping = rdd_row_mapping

      deferred_columns.each { |column| deferred_column column, on_load: ->() {} }

      def self.rdd_row_mapping
        @rdd_row_mapping
      end

      block.call if block
    end
  end

  def generate_composite_model(name, partition, clustering, fields, rdd_row_mapping = nil, deferred_columns = [], &block)
    Class.new(CassandraModel::Record) do
      extend CassandraModel::DataModelling
      self.table_name = name

      model_data do |inquirer, data_set|
        inquirer.knows_about(*partition.keys)
        partition.each { |column, type| inquirer.change_type_of(column).to(type) }

        data_set.is_defined_by(*clustering.keys)
        data_set.knows_about(*fields.keys)
        clustering.merge(fields).each { |column, type| data_set.change_type_of(column).to(type) }
      end

      deferred_columns.each { |column| deferred_column column, on_load: ->() {} }

      table.allow_truncation!
      table.truncate!
      @rdd_row_mapping = rdd_row_mapping

      def self.rdd_row_mapping
        @rdd_row_mapping
      end

      block.call if block
    end
  end

end