package org.apache.spark.api.cassandra_model

import org.luaj.vm2.Globals
import org.luaj.vm2.compiler.LuaC
import org.luaj.vm2._
import org.luaj.vm2.lib.OneArgFunction
import org.luaj.vm2.lib.VarArgFunction
import org.luaj.vm2.LuaTable
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRow
import java.io._

class LuaRDDLib(val env: LuaValue) extends OneArgFunction {
  override def call(arg: LuaValue): LuaValue = {
    val fn_table = new LuaTable()

    env.set("rdd", fn_table)
    return fn_table
  }
}

object LuaRowValue {
  def luaTableToRow(table: LuaTable): Row = {
    val keys = table.keys()
    val length = keys.length
    val row = new Array[Any](length)
    var index = 0

    keys.foreach { table_key =>
      val value = table.get(table_key)
      row(index) = value match {
        case str: LuaString => str.toString()
        case num: LuaInteger => num.toint()
        case fnum: LuaDouble => fnum.tofloat()
      }
      index += 1
    }

    return new GenericRow(row)
  }
}

class LuaRowValue(val schema: StructType, val row: Row) extends LuaValue {
  def `type`(): Int = 999
  def typename(): String = "Row"

  override def tostring() = LuaValue.valueOf(row.toString())
  override def toString() = row.toString()

  override def get(key: LuaValue): LuaValue = {
    val column_index = schema.fieldIndex(key.toString())
    valueOf(row.get(column_index))
  }
  override def get(key: Int): LuaValue = valueOf(row.get(key))

  def valueOf(arg: Any) = {
    arg match {
      case str: String => LuaValue.valueOf(str)
      case num: Int => LuaValue.valueOf(num)
      case fnum: Float => LuaValue.valueOf(fnum)
    }
  }
}

class LuaRDD (val schema: StructType, val rdd: RDD[Row]) extends Serializable {
  def map(new_schema: StructType, lua_code: String): LuaRDD = {
    val new_rdd = rdd.map(callMapScript(lua_code, _))
    new LuaRDD(new_schema, new_rdd)
  }

  def filter(lua_code: String): LuaRDD = {
    val new_rdd = rdd.filter(callFilterScript(lua_code, _))
    new LuaRDD(schema, new_rdd)
  }

  def groupByString(lua_code: String): LuaRDD = {
    val new_schema = groupBySchema(StringType)
    val new_rdd = rdd.groupBy(callGroupByStringScript(lua_code, _))
    val grouped_rdd = groupedRDD(new_rdd)

    new LuaRDD(new_schema, grouped_rdd)
  }

  def groupByInt(lua_code: String): LuaRDD = {
    val new_schema = groupBySchema(IntegerType)
    val new_rdd = rdd.groupBy(callGroupByIntScript(lua_code, _))
    val grouped_rdd = groupedRDD(new_rdd)

    new LuaRDD(new_schema, grouped_rdd)
  }

  def groupByFloat(lua_code: String): LuaRDD = {
    val new_schema = groupBySchema(FloatType)
    val new_rdd = rdd.groupBy(callGroupByFloatScript(lua_code, _))
    val grouped_rdd = groupedRDD(new_rdd)

    new LuaRDD(new_schema, grouped_rdd)
  }

  def toDF(sql_context: SQLContext) = sql_context.createDataFrame(rdd, schema)

  private def groupBySchema(data_type: DataType): StructType = {
    val fields = Array(StructField("key", data_type), StructField("values", schema))
    StructType(fields)
  }

  private def groupedRDD[T](rdd: RDD[(T, Iterable[Row])]): RDD[Row] = {
    rdd.map { case (key, values) =>
      val row: Array[Any] = Array(key, values)
      new GenericRow(row)
    }
  }

  private def callScript(lua_code: String, row: Row): LuaValue = {
    val globals = new Globals()
    LuaC.install(globals)
    globals.set("ROW", new LuaRowValue(schema, row))

    val chunk = globals.load(lua_code)
    chunk.call()
  }

  private def callMapScript(lua_code: String, row: Row): Row = {
    callScript(lua_code, row) match {
      case row: LuaRowValue => row.row
      case table: LuaTable => LuaRowValue.luaTableToRow(table)
    }
  }

  private def callFilterScript(lua_code: String, row: Row): Boolean = {
    callScript(lua_code, row) match {
      case bool: LuaBoolean => bool.toboolean()
    }
  }

  private def callGroupByStringScript(lua_code: String, row: Row): String = {
    callScript(lua_code, row) match {
      case str: LuaString => str.toString()
    }
  }

  private def callGroupByIntScript(lua_code: String, row: Row): Int = {
    callScript(lua_code, row) match {
      case num: LuaInteger => num.toint()
    }
  }

  private def callGroupByFloatScript(lua_code: String, row: Row): Float = {
    callScript(lua_code, row) match {
      case fnum: LuaDouble => fnum.tofloat()
    }
  }

}
