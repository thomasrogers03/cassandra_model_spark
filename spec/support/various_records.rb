module VariousRecords
  extend RSpec::SharedContext

  let(:basic_record_klass) do
    Class.new(CassandraModel::Record) do
      name = :"basic_#{Faker::Lorem.word}"
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
    end
  end
  let(:composite_record_klass) do
    Class.new(CassandraModel::Record) do
      extend CassandraModel::DataModelling
      self.table_name = :"composite_#{Faker::Lorem.word}"
      model_data do |inquirer, data_set|
        inquirer.knows_about(:key)
        data_set.is_defined_by(:value)
        data_set.knows_about(:description, :extra_data)
        data_set.change_type_of(:extra_data).to(:blob)
      end
    end
  end

end