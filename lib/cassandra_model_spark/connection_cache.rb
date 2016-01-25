module CassandraModel
  class ConnectionCache
    def self.clear
      @@cache.values.select(&:has_spark_context?).map(&:java_spark_context).map(&:stop)
      @@cache.values.map(&:shutdown)
      @@cache.clear
    end
  end
end
