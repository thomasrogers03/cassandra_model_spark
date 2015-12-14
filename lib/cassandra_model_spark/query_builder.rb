module CassandraModel
  class QueryBuilder
    def as_data_frame
      Spark::DataFrame.new(@record_klass, nil)
    end
  end
end
