require 'guard/compat/plugin'

module ::Guard
  class ScalaHelper < Plugin

    def run_all
      build
    end

    def run_on_modifications(paths)
      build
    end

    private

    def build
      Process.wait(Process.spawn('./bin/cmodel-spark-build -e'))
    end
  end
end

guard :rspec, cmd: 'bundle exec rspec' do
  watch(%r{^spec/.+_spec\.rb$})
  watch('spec/spec_helper.rb') { 'spec' }
  watch(%r{^lib/(.+)\.rb$}) { |m| "spec/lib/#{m[1]}_spec.rb" }
  watch(%r{^lib/cassandra_model_spark\.rb$}) { "spec" }
  watch(%r{^lib/cassandra_model_spark/(.+)\.rb}) { |m| "spec/classes/#{m[1]}_spec.rb" }
  watch(%r{^spec/shared_examples/(.+)\.rb}) { "spec" }
  watch(%r{^spec/helpers/(.+)\.rb}) { "spec" }
  watch(%r{^spec/support/(.+)\.rb}) { "spec" }
  watch(%r{^spec/support/(.+)\.rb$}) { "spec" }
end

group :connection do
  guard :rspec, cmd: 'bundle exec rspec', spec_paths: %w(connection_spec) do
    watch(%r{^connection_spec/classes/.+_spec\.rb$})
    watch(%r{^lib/cassandra_model_spark/(.+)\.rb}) { |m| "connection_spec/classes/#{m[1]}_spec.rb" }
    watch('spec/connection_spec_helper.rb') { 'connection_spec' }
  end
end

group :scala do
  guard :scala_helper do
    watch(%r{^ext/scala_helper/.+\.scala$})
  end

  guard :rspec, cmd: 'bundle exec rspec', spec_paths: %w(ext/scala_helper/spec) do
    watch(%r{^ext/scala_helper/spec/classes/.+_spec\.rb$})
    watch(%r{^ext/scala_helper/(.+)\.scala$}) { |m| "ext/scala_helper/spec/classes/#{m[1]}_spec.rb" }
    watch('spec/scala_spec_helper.rb') { 'ext/scala_helper/spec' }
  end
end

guard :bundler do
  require 'guard/bundler'
  require 'guard/bundler/verify'
  helper = Guard::Bundler::Verify.new

  files = ['Gemfile']
  files += Dir['*.gemspec'] if files.any? { |f| helper.uses_gemspec?(f) }

  # Assume files are symlinked from somewhere
  files.each { |file| watch(helper.real_path(file)) }
end
