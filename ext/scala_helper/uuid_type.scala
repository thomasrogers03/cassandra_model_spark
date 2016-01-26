package org.apache.spark.api.cassandra_model

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

@SQLUserDefinedType(udt = classOf[UUIDType])
class SqlUUID(val uuid: String) extends Serializable

class UUIDType extends UserDefinedType[SqlUUID] {
  override def sqlType: DataType = StringType

  override def serialize(obj: Any) = {
    obj match {
      case u: SqlUUID => Row(u.uuid)
    }
  }

  override def deserialize(datum: Any): SqlUUID = {
    datum match {
      case Row(s: String) => new SqlUUID(s)
    }
  }

  override def userClass: Class[SqlUUID] = classOf[SqlUUID]

  private[spark] override def asNullable: UUIDType = this
}

case object UUIDType extends UUIDType
