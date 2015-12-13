class RDD < Array
  attr_reader :context

  def initialize(context, *array_args)
    @context = context
    super(*array_args)
  end
end

class SparkCassandraHelper
  #noinspection RubyUnusedLocalVariable
  def self.cassandra_table(context, keyspace, table)
    RDD.new(context)
  end
end