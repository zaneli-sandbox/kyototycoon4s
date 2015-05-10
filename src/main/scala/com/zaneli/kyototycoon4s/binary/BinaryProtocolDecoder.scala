package com.zaneli.kyototycoon4s.binary

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ByteToMessageDecoder

class BinaryProtocolDecoder extends ByteToMessageDecoder {
  private[this] val magic: Byte = 0xBA.toByte
  private[this] val zero: Byte = 0.toByte

  override def decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: java.util.List[AnyRef]): Unit = {
    if (buf.isReadable && (buf.readByte() == magic)) {
      val hits = buf.readUnsignedInt().toInt
      (1 to hits) map { _ =>
        val dbidx = buf.readUnsignedShort()
        val ksiz = buf.readUnsignedInt().toInt
        val vsiz = buf.readUnsignedInt().toInt
        val xt = buf.readLong()
        val key = readString(buf, ksiz)
        val value = readString(buf, vsiz)
        println((key, value))
        out.add((key, value))
      }
      if (!buf.isReadable) {
        ctx.close()
      }
    }
  }

  private[this] def readString(buf:ByteBuf, size: Int): String = {
    val bytes = Array.fill[Byte](size)(zero)
    buf.readBytes(bytes)
    new String(bytes, "UTF-8")
  }
}
