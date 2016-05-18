class String
  def toString
    self
  end
end

class NilClass
  alias :toString :to_s
end

module CassandraModel
  module Spark
    module Lib

    end
  end
end

if RUBY_ENGINE == 'jruby'
  class Hash
    def to_java
      CassandraModel::Spark::Lib::JavaHashMap.new(self)
    end
  end

  class Array
    def self.from_java_array_list(array_list)
      array_list.to_a
    end

    def to_java_argv
      to_java(:string)
    end
  end

  class String
    def self.from_java_string(string)
      string
    end
  end

else
  class Hash
    def to_java
      CassandraModel::Spark::Lib::JavaHashMap.new.tap do |map|
        each do |key, value|
          map.put(key, value)
        end
      end
    end
  end

  class Array
    def self.from_java_array_list(array_list)
      array_list.toArray
    end

    def to_java
      self
    end

    def to_java_argv
      self
    end
  end

  class String
    def self.from_java_string(string)
      string.toString
    end
  end
end

module JavaBridge
  if RUBY_ENGINE == 'jruby'
    def import_java_object(path, options = {})
      name = options.fetch(:as) { path.split('.').last }.to_sym
      klass = "Java::#{path}"
      set_import_const(name, eval(klass))
    end

    def initialize_java_engine
      # nothing to do here
    end
  else
    def import_java_object(path, options = {})
      name = options.fetch(:as) { path.split('.').last }.to_sym
      set_import_const(name, load_java_class(path))
    end

    def require(path)
      # hack to make importing jars work like jruby
      if path =~ /\.jar$/i
        java_jar_list << path
      else
        super
      end
    end

    def initialize_java_engine
      # have to load everything in one go here
      Rjb.load(java_jar_list * platform_path_separator)
    end

    private

    def platform_path_separator
      @platform_separator ||= RbConfig::CONFIG['host_os'] =~ /mswin|mingw/ ? ';' : ':'
    end

    def java_jar_list
      @java_jar_list ||= []
    end

    def load_java_class(path)
      import_quiet { Rjb.import(path) }
    end
  end

  private

  def set_import_const(name, value)
    CassandraModel::Spark::Lib.const_set(name, value)
  end

  public

  def import_quiet
    prev_verbox = $VERBOSE
    $VERBOSE = nil
    yield
  ensure
    $VERBOSE = prev_verbox
  end
end

include JavaBridge
