package org.apache.spark.sql.types

import scala.math.Ordering
import scala.reflect.runtime.universe.typeTag

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.ScalaReflectionLock
import java.util.UUID

@DeveloperApi
class UUIDType private() extends AtomicType {
  private[sql] type InternalType = UUID
  @transient private[sql] lazy val tag = ScalaReflectionLock.synchronized { typeTag[InternalType] }
  private[sql] val ordering = implicitly[Ordering[InternalType]]

  override def defaultSize: Int = 36

  private[spark] override def asNullable: UUIDType = this
}

case object UUIDType extends UUIDType
