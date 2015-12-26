class JavaArray < Struct.new(:values)
end

class Array
  def to_java
    JavaArray.new(self)
  end
end