module CassandraModel
  class QueryBuilder
    def group(*columns)
      append_option(columns, :group)
    end

    def as_data_frame(options = {})
      if @record_klass.is_a?(Spark::DataFrame)
        data_frame_from_frame(options)
      else
        data_frame_from_model(options)
      end
    end

    private

    def data_frame_from_frame(options)
      query_frame = @record_klass.query(@params, @options)
      Spark::DataFrame.new(@record_klass.record_klass, nil, options.merge(spark_data_frame: query_frame))
    end

    def data_frame_from_model(options)
      updated_restriction = @record_klass.restriction_attributes(@params).inject({}) do |memo, (key, value)|
        updated_key = if value.is_a?(Array)
                        value = value.to_java
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
