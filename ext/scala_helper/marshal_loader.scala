package org.apache.spark.api.cassandra_model

import scala.collection.mutable._

class MarshalLoader (dump: Array[Byte]) {
  private val bytes: Array[Byte] = dump
  private var parse_index: Int = 0
  private var symbol_table: List[String] = List()
  private var object_table: List[AnyRef] = List()

  private def getBytes() = {
    bytes
  }

  private def recordObject(obj: AnyRef) = {
    object_table :+= obj
  }

  private def nextBytes(amount: Int): Array[Byte] = {
    val result = bytes.slice(parse_index, parse_index + amount)
    parse_index = parse_index + amount
    result
  }

  private def nextByte(): Byte = {
    val result = bytes(parse_index)

    parse_index = parse_index + 1

    result
  }

  private def scanIntBits(num_bytes: Int) = {
    var bit: Int = 0
    var value: Int = 0

    for (bit <- 0 to num_bytes-1) {
      val next_value = 0xff & nextByte()
      value += (next_value << (bit * 8))
    }
    value
  }

  private def decodeInt(): java.lang.Integer = {
    val first_byte: Int = nextByte()
    var value: Int = 0

    if (first_byte == 0)
      return 0
    if (first_byte >= 6)
      return first_byte - 5
    if (first_byte <= -6)
      return 5 + first_byte

    var num_bytes = first_byte
    if (num_bytes > 0) {
      value = scanIntBits(num_bytes)
    } else {
      num_bytes = -num_bytes
      value = scanIntBits(num_bytes)
      val long_bytes = num_bytes
      val long_value: Long = value - (1L << (long_bytes * 8L))
      value = long_value.toInt
    }

    value
  }

  private def decodeDouble(): java.lang.Double = {
    val length = decodeInt()
    val str_value = new String(nextBytes(length))

    val result: java.lang.Double = str_value.toDouble
    recordObject(result)
    result
  }

  private def decodeASCIIString(): String = {
    val length = decodeInt()

    val result = new String(nextBytes(length))
    recordObject(result)
    result
  }

  private def decodeSymbol(): String = {
    val length = decodeInt()
    val string_bytes = nextBytes(length)
    val result = new String(string_bytes)

    symbol_table :+= result

    result
  }

  private def decodeSymLink(): String = {
    val index = decodeInt()

    symbol_table(index)
  }

  private def decodeString(): String = {
    val string_code = nextByte()
    val length = decodeInt()
    val str_bytes = nextBytes(length)
    val var_count = decodeInt()
    val encoding = decodeAny()

    if (encoding == "E") {
      val is_utf8 = decodeAny()
    }

    val result = new String(str_bytes)
    recordObject(result)
    result
  }

  private def decodeMagic(): String = {
    val magic = new String(nextBytes(4))

    if (magic != "MRSH") {
      throw new IllegalArgumentException("Invalid format header: '" + magic + "'")
    }

    magic
  }

  private def decodeVersion(): Array[Byte] = {
    val version = nextBytes(2)

    if (version(0) != 0x04 || version(1) != 0x08) {
      throw new IllegalArgumentException("Invalid Marshal version: [" + version(0) + "], [" + version(1) + "]")
    }

    version
  }

  private def decodeHashItem(): String = {
    val ivar_code = nextByte()
    decodeString()
  }

  private def decodeHash(): HashMap[AnyRef, AnyRef] = {
    var result = new HashMap[AnyRef, AnyRef]
    val length = decodeInt()

    var item = 0
    for (item <- 0 to length-1) {
      val key = decodeAny()
      val value = decodeAny()
      result(key) = value
    }

    recordObject(result)
    result
  }

  private def decodeArray(): Array[AnyRef] = {
    var list_result: List[AnyRef] = List()
    val length = decodeInt()

    var item = 0
    for (item <- 0 to length-1) {
      val value = decodeAny()
      list_result :+= value
    }

    val result = list_result.toArray
    recordObject(result)
    result
  }

  private def decodeObjectReference(): AnyRef = {
      val index = decodeInt()-1

      object_table(index)
  }

  private def decodeAny(): AnyRef = {
    val code = nextByte()

    code match {
      case 0x30 => null
      case 0x54 => true: java.lang.Boolean
      case 0x46 => false: java.lang.Boolean
      case 0x69 => decodeInt()
      case 0x66 => decodeDouble()
      case 0x3a => decodeSymbol()
      case 0x3b => decodeSymLink()
      case 0x7b => decodeHash()
      case 0x5b => decodeArray()
      case 0x22 => decodeASCIIString()
      case 0x49 => decodeString()
      case 0x40 => decodeObjectReference()
      case _ => throw new IllegalArgumentException("Unsupported code type: " + code)
    }
  }

  private val magic = decodeMagic()
  private val version = decodeVersion()
  private val value = decodeAny()

  def getValue(): AnyRef = value
}
