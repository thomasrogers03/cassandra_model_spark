module CassandraModel
  class QueryBuilder
    def as_data_frame(options = {})
      updated_restriction = @record_klass.restriction_attributes(@params).inject({}) do |memo, (key, value)|
        updated_key = if value.is_a?(Array)
                        updated_key = key.is_a?(ThomasUtils::KeyComparer) ? key.to_s : "#{key} IN"
                        "#{updated_key} (#{(%w(?)*value.count)*','})"
                      else
                        key.is_a?(ThomasUtils::KeyComparer) ? "#{key} ?" : "#{key} = ?"
                      end
        memo.merge!(updated_key => value)
      end.stringify_keys.to_java
      rdd = SparkCassandraHelper.filterRDD(@record_klass.rdd, updated_restriction)
      Spark::DataFrame.new(@record_klass, rdd, options)
    end
  end
end
