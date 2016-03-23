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

  def generate_simple_model(name, partition, clustering, fields)
    column_types = (partition + clustering + fields).map { |key| "#{key} text" } * ', '
    partition = "(#{partition * ', '})"
    clustering = "#{clustering * ', '}"
    create_query = "CREATE TABLE IF NOT EXISTS #{name} (#{column_types}, PRIMARY KEY (#{partition}, #{clustering}))"
    CassandraModel::ConnectionCache[nil].session.execute(create_query)
    Class.new(CassandraModel::Record) do
      self.table_name = name
      table.allow_truncation!
      table.truncate!
    end
  end

end