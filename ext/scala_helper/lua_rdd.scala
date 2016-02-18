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
import scala.reflect.ClassTag

class LuaRDDLib(val env: LuaValue) extends OneArgFunction {
  override def call(arg: LuaValue): LuaValue = {
    val fn_table = new LuaTable()

    env.set("rdd", fn_table)
    return fn_table
  }
}

object LuaRowValue {
  def luaTableToArray[T](table: LuaTable)(implicit m: ClassTag[T]): Array[T] = {
    val keys = table.keys()
    val length = keys.length
    val result = new Array[T](length)
    var index = 0

    keys.foreach { table_key =>
      val value = table.get(table_key)
      val result_value = value match {
        case str: LuaString => str.toString()
        case num: LuaInteger => num.toint()
        case fnum: LuaDouble => fnum.tofloat()
      }
      result(index) = result_value match { case t_value: T => t_value }
      index += 1
    }
    result
  }

  def luaTableToRow(table: LuaTable): Row = {
    val row: Array[Any] = luaTableToArray(table)
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

class PartitionableStringArray(val items: Array[String]) extends Serializable{
  override val hashCode = {
    val some_prime = 31
    var result = 1

    for(str <- items) {
      result = result * some_prime + str.hashCode
    }
    result
  }

  override def equals(rhs: Any) = {
    rhs match {
      case string_array: PartitionableStringArray => items == string_array.items
      case _ => false
    }
  }
}

object LuaRDD {
  private val thread_local_globals = new ThreadLocal[Globals]

  def getGlobals(): Globals = thread_local_globals.get()
  def newGlobals(): Globals = {
    val globals = new Globals()
    LuaC.install(globals)
    thread_local_globals.set(globals)
    globals
  }

  def getGlobalsOrNew(): Globals = {
    var globals = getGlobals()
    if(globals == null)
      globals = newGlobals()
    globals
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

  def groupByStringArray(lua_code: String): LuaRDD = {
    val new_schema = groupBySchema(ArrayType(StringType))
    val pre_rdd = rdd.groupBy(callGroupByStringArrayScript(lua_code, _))
    val new_rdd: RDD[(Array[String], Iterable[Row])] = pre_rdd.map { case(key, values) =>
      (key.items, values)
    }
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
    val fields = Array(StructField("key", data_type), StructField("values", ArrayType(schema)))
    StructType(fields)
  }

  private def groupedRDD[T](rdd: RDD[(T, Iterable[Row])]): RDD[Row] = {
    rdd.map { case (key, values) =>
      val row: Array[Any] = Array(key, values.toArray)
      new GenericRow(row)
    }
  }

  private def callScript(lua_code: String, row: Row): LuaValue = {
    val globals = LuaRDD.getGlobalsOrNew()
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

  private def callGroupByStringArrayScript(lua_code: String, row: Row): PartitionableStringArray = {
    callScript(lua_code, row) match {
      case table: LuaTable => new PartitionableStringArray(LuaRowValue.luaTableToArray(table))
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
