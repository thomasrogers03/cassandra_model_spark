Gem::Specification.new do |s|
  s.name = 'cassandra_model_spark'
  s.version = '0.0.1'
  s.license = 'Apache License 2.0'
  s.summary = 'Spark integration for cassandra_model'
  s.description = %q{Spark integration for cassandra_model.
Get high-performance data analytics with the ease of cassandra_model.
Inspired by the ruby-spark gem.}
  s.authors = ['Thomas RM Rogers']
  s.email = 'thomasrogers03@gmail.com'
  s.files = Dir['{lib}/**/*.rb', 'bin/*', 'LICENSE.txt', '*.md']
  s.require_path = 'lib'
  s.homepage = 'https://www.github.com/thomasrogers03/cassandra_model_spark'
  s.add_runtime_dependency 'cassandra_model', '~> 0.9.15'
  if RUBY_ENGINE != 'jruby'
    s.add_runtime_dependency 'rjb', '~> 1.5.4'
  end
end
