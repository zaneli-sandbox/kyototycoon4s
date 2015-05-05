package com.zaneli.kyototycoon4s.rpc

sealed abstract class Encoder {
  def colenc: Option[String]
  def encode(s: String): String
  def encode(bs: Array[Byte]): String
}

object Encoder {
  object None extends Encoder {
    override val colenc = scala.None
    override def encode(s: String): String = s
    override def encode(bs: Array[Byte]): String = new String(bs, "UTF-8")
  }

  object Base64 extends Encoder {
    import org.apache.commons.codec.binary.{Base64 => Base64Codec}
    import scalaj.http.HttpConstants
    override val colenc = Some("B")
    override def encode(s: String): String = HttpConstants.base64(s)
    override def encode(bs: Array[Byte]): String = HttpConstants.base64(bs)
  }

  object URL extends Encoder {
    import org.apache.commons.codec.net.URLCodec
    private[this] lazy val codec = new URLCodec()
    override val colenc = Some("U")
    override def encode(s: String): String = codec.encode(s)
    override def encode(bs: Array[Byte]): String = new String(codec.encode(bs), "UTF-8")
  }
}
