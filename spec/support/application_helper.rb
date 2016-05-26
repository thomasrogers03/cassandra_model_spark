module ApplicationHelper
  extend RSpec::SharedContext

  before do
    allow(CassandraModel::Spark).to receive(:application) do
      new CassandraModel::Spark::Application({})
    end
  end
end