Gem::Specification.new do |s|
  s.name = 'cassandra_model_spark'
  s.version = '0.0.5'
  s.license = 'Apache License 2.0'
  s.summary = 'Spark integration for cassandra_model'
  s.description = %q{Spark integration for cassandra_model.
Get high-performance data analytics with the ease of cassandra_model.
Inspired by the ruby-spark gem.}
  s.authors = ['Thomas RM Rogers']
  s.email = 'thomasrogers03@gmail.com'
  s.files = Dir['{lib}/**/*.rb', 'bin/*',
                'ext/scala_helper/*.scala',
                'ext/scala_helper/**/*.sbt',
                'ext/scala_helper/bin/*',
                'ext/scala_helper/sbin/*',
                'LICENSE.txt',
                '*.md']
  s.require_path = 'lib'
  s.homepage = 'https://www.github.com/thomasrogers03/cassandra_model_spark'
  s.add_runtime_dependency 'cassandra_model', '~> 1.1'
  s.add_runtime_dependency 'thomas_utils', '>= 0.1.16', '< 0.3.0'
  s.add_runtime_dependency 'connection_pool', '~> 2.2.0'
  s.add_runtime_dependency 'jruby-kafka', '~> 3.6.0'
  s.platform = 'java'

  s.executables << 'cmodel-spark-build'
  s.executables << 'cmodel-spark-env.rb'
  s.executables << 'cmodel-spark-master'
  s.executables << 'cmodel-spark-slaves'
  s.executables << 'cmodel-spark-run-master'
  s.executables << 'cmodel-spark-run-slave'
end
