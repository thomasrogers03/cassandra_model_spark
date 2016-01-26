package org.apache.spark.api.cassandra_model

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

@SQLUserDefinedType(udt = classOf[TimeUUIDType])
class SqlTimeUUID(val uuid: String) extends Serializable

class TimeUUIDType extends UserDefinedType[SqlTimeUUID] {
  override def sqlType: DataType = StringType

  override def serialize(obj: Any) = {
    obj match {
      case t: SqlTimeUUID => Row(t.uuid)
      case u: SqlUUID => Row(u.uuid)
    }
  }

  override def deserialize(datum: Any): SqlTimeUUID = {
    datum match {
      case Row(s: String) => new SqlTimeUUID(s)
    }
  }

  override def userClass: Class[SqlTimeUUID] = classOf[SqlTimeUUID]

  private[spark] override def asNullable: TimeUUIDType = this
}

case object TimeUUIDType extends TimeUUIDType
