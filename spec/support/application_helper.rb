module ApplicationHelper
  extend RSpec::SharedContext

  before do
    allow(CassandraModel::Spark).to receive(:application) do
      CassandraModel::Spark::Application.new({})
    end
  end
end