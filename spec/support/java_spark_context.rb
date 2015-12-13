class SparkConf
  extend Forwardable

  def_delegator :@conf, :[]=, :set
  def_delegator :@conf, :[], :get
  def_delegator :@conf, :hash

  def self.from_hash(hash)
    new(true).tap do |conf|
      hash.each { |key, value| conf.set(key, value) }
    end
  end

  def initialize(_)
    @conf = {}
  end

  def ==(rhs)
    rhs.is_a?(SparkConf) && conf == rhs.conf
  end

  def eql?(rhs)
    self == rhs
  end

  protected

  attr_reader :conf
end

class JavaSparkContext < Struct.new(:config)
end