package org.apache.spark.api.cassandra_model

import org.luaj.vm2.Globals
import org.luaj.vm2.compiler.LuaC
import org.luaj.vm2._
import org.luaj.vm2.lib.OneArgFunction
import org.luaj.vm2.lib.VarArgFunction
import org.luaj.vm2.LuaTable
import org.apache.spark.rdd._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import java.io._

class LuaRDDLib(val env: LuaValue) extends OneArgFunction {
  override def call(arg: LuaValue): LuaValue = {
    val fn_table = new LuaTable()

    env.set("rdd", fn_table)
    return fn_table
  }
}

object LuaRowValue {
  def fromLuaTable(table: LuaTable): Row = {
    val keys = table.keys()
    val length = keys.length
    val schema = new Array[StructField](length)
    val row = new Array[Any](length)
    var index = 0

    keys.foreach { table_key =>
      val value = table.get(table_key)
      row(index) = value match {
        case str: LuaString => str.toString()
        case num: LuaInteger => num.toint()
        case fnum: LuaDouble => fnum.tofloat()
      }
      schema(index) = StructField(table_key.toString(), value match {
        case str: LuaString => StringType
        case num: LuaInteger => IntegerType
        case fnum: LuaDouble => FloatType
      })
      index += 1
    }

    return new GenericRowWithSchema(row, StructType(schema))
  }
}

class LuaRowValue(val row: Row) extends LuaValue {
  val schema = row.schema

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

object LuaRDD {

  def callScript(lua_code: String, row: Row): LuaValue = {
    val globals = new Globals()
    LuaC.install(globals)
    globals.set("ROW", new LuaRowValue(row))

    val chunk = globals.load(lua_code)
    return chunk.call()
  }

  def callMapScript(lua_code: String, row: Row): Row = {
    return callScript(lua_code, row) match {
      case row: LuaRowValue => row.row
      case table: LuaTable => LuaRowValue.fromLuaTable(table)
    }
  }

  def callFilterScript(lua_code: String, row: Row): Boolean = {
    return callScript(lua_code, row) match {
      case bool: LuaBoolean => bool.toboolean()
    }
  }

  def callGroupByStringScript(lua_code: String, row: Row): String = {
    return callScript(lua_code, row) match {
      case str: LuaString => str.toString()
    }
  }

  def callGroupByIntScript(lua_code: String, row: Row): Int = {
    return callScript(lua_code, row) match {
      case num: LuaInteger => num.toint()
    }
  }

  def callGroupByFloatScript(lua_code: String, row: Row): Float = {
    return callScript(lua_code, row) match {
      case fnum: LuaDouble => fnum.tofloat()
    }
  }

}

class LuaRDD (var rdd: RDD[Row]) {
  def map(lua_code: String) = {
    new LuaRDD(rdd.map(LuaRDD.callMapScript(lua_code, _)))
  }

  def filter(lua_code: String) = {
    new LuaRDD(rdd.filter(LuaRDD.callFilterScript(lua_code, _)))
  }

  def groupByString(lua_code: String) = {
    rdd.groupBy(LuaRDD.callGroupByStringScript(lua_code, _))
  }
}
