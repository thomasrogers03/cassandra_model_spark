if RUBY_PLATFORM == 'java'
  class Java::JavaUtil::ArrayList
    alias :inspect :toString
  end
else
  class JavaArray < Struct.new(:values)
  end

  class Array
    def to_java
      JavaArray.new(self)
    end
  end
end