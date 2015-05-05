package com.zaneli.kyototycoon4s

import java.nio.ByteBuffer

object Implicits {
  implicit val str2Bytes: (String => Array[Byte]) = _.getBytes("UTF-8")

  implicit val long2Bytes: (Long => Array[Byte]) = ByteBuffer.allocate(8).putLong(_).array
}
