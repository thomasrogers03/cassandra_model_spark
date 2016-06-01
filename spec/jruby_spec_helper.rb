raise NotImplementedError, 'Unsupported platform, please use JRuby' unless RUBY_PLATFORM == 'java'

require 'rspec'

if ENV['COVERAGE'].to_i > 0
  require 'simplecov'
  SimpleCov.start do
    add_filter '/spec/'
    add_filter '/examples/'
  end
end

module CassandraModel
  NO_BRIDGE = true
end

require 'bundler'
Dir['./spec/support/**.rb'].each { |file| require file }
require 'cassandra_mocks'
Bundler.require(:default, :development, :test)
Dir['./spec/helpers/**.rb'].each { |file| require file }
Dir['./spec/shared_examples/**.rb'].each { |file| require file }

RSpec.configure do |config|
  require_relative '../lib/cassandra_model_spark'

  config.include ApplicationHelper
  config.include VariousRecords

  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.before do
    CassandraModel::ConnectionCache.clear
    CassandraModel::TableDescriptor.create_descriptor_table
  end

  config.order = :random
end
