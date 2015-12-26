if RUBY_ENGINE == 'jruby'
  class Hash
    def to_java
      JavaHashMap.new(self)
    end
  end
else
  class Hash
    def to_java
      JavaHashMap.new.tap do |map|
        each do |key, value|
          map.put(key, value)
        end
      end
    end
  end

  class Array
    def to_java
      self
    end
  end
end

module JavaBridge
  if RUBY_ENGINE == 'jruby'
    def import_java_object(path, options = {})
      name = options.fetch(:as) { path.split('.').last }.to_sym
      klass = "Java::#{path}"
      Object.const_set(name, eval(klass))
    end

    def initialize_java_engine
      # nothing to do here
    end
  else
    def import_java_object(path, options = {})
      name = options.fetch(:as) { path.split('.').last }.to_sym
      Object.const_set(name, load_java_class(path))
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

  def import_quiet
    prev_verbox = $VERBOSE
    $VERBOSE = nil
    yield
  ensure
    $VERBOSE = prev_verbox
  end
end

include JavaBridge
