package com.zaneli.kyototycoon4s.binary

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToByteEncoder

class BinaryProtocolEncoder extends MessageToByteEncoder[Seq[String]] {

  private[this] val magic: Int = 0xBA
  private[this] lazy val flag: Array[Byte] = Array.fill(4)(0.toByte)

  override def encode(ctx: ChannelHandlerContext, keys: Seq[String], buf: ByteBuf): Unit = {
    println("encode")
    buf.writeByte(magic)
    buf.writeBytes(flag)

    val rnum = keys.size
    writeBytes(buf, keys.size, 32)

    keys.foreach { key =>
      val dbidx = 0
      writeBytes(buf, dbidx, 16)

      val ksiz = key.length
      writeBytes(buf, ksiz, 32)

      buf.writeBytes(key.getBytes("UTF-8"))
    }
  }

  private[this] def writeBytes(buf: ByteBuf, num: Int, unit: Int): Unit = {
    ((unit / 8) - 1).to(0, -1) foreach { i =>
      val b = (num >>> (8 * i)) & 0xFF
      buf.writeByte(b)
    }
  }
}
