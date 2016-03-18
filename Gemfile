source 'https://rubygems.org'

group :development do
  gem 'rdoc'
  gem 'cassandra-driver', '~> 1.1', require: false
  gem 'activesupport', require: false
  gem 'concurrent-ruby', require: false
  gem 'thomas_utils', '~> 0.1.4', github: 'thomasrogers03/thomas_utils', require: false
  gem 'batch_reactor', github: 'thomasrogers03/batch_reactor', require: false
  gem 'cassandra_model', github: 'thomasrogers03/cassandra_model', require: false
  gem 'rjb', platform: :ruby, require: false
  gem 'pry'
end

group :test do
  gem 'rspec', '~> 3.1.0', require: false
  gem 'rspec-its'
  gem 'guard-rspec'
  gem 'guard-bundler'
  gem 'guard'
  gem 'timecop'
  gem 'simplecov', require: false
  gem 'faker'
end

gemspec(name: 'cassandra_model_spark')
