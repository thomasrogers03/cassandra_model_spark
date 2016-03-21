package org.apache.spark.api.cassandra_model

import org.luaj.vm2._
import org.luaj.vm2.lib._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

class LuaRowLib extends TwoArgFunction {
  override def call(mod_name: LuaValue, env: LuaValue): LuaValue = {
    val fn_table = new LuaTable()

    fn_table.set("append", new append())
    fn_table.set("replace", new replace())
    fn_table.set("slice", new slice())

    env.set("row", fn_table)
    fn_table
  }

  class append extends LibFunction {
    override def call(lua_row: LuaValue, lua_key: LuaValue, lua_value: LuaValue): LuaValue = {
      val row = lua_row match {
        case row: LuaRowValue => row
      }
      val key: String = lua_key match {
        case str: LuaString => str.toString()
      }
      val value = lua_value match {
        case str: LuaString => str.toString()
        case num: LuaInteger => num.toint()
        case dfnum: LuaDouble => dfnum.todouble()
      }
      val data_type = value match {
        case str: String => StringType
        case num: Int => IntegerType
        case dfnum: Double => DoubleType
      }
      val fields = row.schema.fields :+ StructField(key, data_type)
      val new_schema = StructType(fields)
      val new_values = row.row.toSeq :+ value
      val new_row = Row.fromSeq(new_values)

      new LuaRowValue(new_schema, new_row)
    }
  }

  class replace extends LibFunction {
    override def call(lua_row: LuaValue, lua_key: LuaValue, lua_value: LuaValue): LuaValue = {
      val row = lua_row match {
        case row: LuaRowValue => row
      }
      val key: String = lua_key match {
        case str: LuaString => str.toString()
      }
      val value = lua_value match {
        case str: LuaString => str.toString()
        case num: LuaInteger => num.toint()
        case dfnum: LuaDouble => dfnum.todouble()
      }
      val data_type = value match {
        case str: String => StringType
        case num: Int => IntegerType
        case dfnum: Double => DoubleType
      }
      val schema = row.schema
      val column_index = schema.fieldIndex(key)
      val new_values = row.row.toSeq.updated(column_index, value)
      val new_row = Row.fromSeq(new_values)

      new LuaRowValue(schema, new_row)
    }
  }

  class slice extends LibFunction {
    override def call(lua_row: LuaValue, lua_keys: LuaValue): LuaValue = {
      val row = lua_row match {
        case row: LuaRowValue => row
      }
      val key_list = lua_keys match {
        case list: LuaTable => list
      }
      val keys = (1 to key_list.length).map {
        index: Int => key_list.get(index) match {
          case str: LuaString => str.toString()
        }
      }
      val schema = row.schema
      val new_schema = StructType(keys.map(schema(_)))
      val field_indices = keys.map(schema.fieldIndex(_))
      val new_values = field_indices.map(row.row(_))
      val new_row = Row.fromSeq(new_values)

      new LuaRowValue(schema, new_row)
    }
  }

}

