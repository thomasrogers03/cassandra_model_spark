module ApplicationHelper
  extend RSpec::SharedContext

  let(:global_application) { CassandraModel::Spark::Application.new({}) }

  before do
    allow(CassandraModel::Spark).to receive(:application).and_return(global_application)
  end
end