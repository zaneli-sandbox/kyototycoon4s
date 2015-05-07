package com.zaneli.kyototycoon4s.rpc

sealed abstract class Encoder {
  def colenc: Option[String]

  def encode(s: String): String
  def encode(bs: Array[Byte]): String

  /** must override either decode method */
  def decode(s: String): Array[Byte] = decodeString(s).getBytes("UTF-8")
  def decodeString(s: String): String = new String(decode(s), "UTF-8")
}

object Encoder {
  private[this] lazy val regex = """.+; colenc=(\w)""".r

  object None extends Encoder {
    override val colenc = scala.None
    override def encode(s: String): String = s
    override def encode(bs: Array[Byte]): String = new String(bs, "UTF-8")
    override def decodeString(s: String): String = s
  }

  object Base64 extends Encoder {
    import org.apache.commons.codec.binary.{Base64 => Base64Codec}
    import scalaj.http.HttpConstants
    override val colenc = Some("B")
    override def encode(s: String): String = HttpConstants.base64(s)
    override def encode(bs: Array[Byte]): String = HttpConstants.base64(bs)
    override def decode(s: String): Array[Byte] = Base64Codec.decodeBase64(s)
  }

  object URL extends Encoder {
    import org.apache.commons.codec.net.URLCodec
    private[this] lazy val codec = new URLCodec()
    override val colenc = Some("U")
    override def encode(s: String): String = codec.encode(s)
    override def encode(bs: Array[Byte]): String = new String(codec.encode(bs), "UTF-8")
    override def decodeString(s: String): String = codec.decode(s)
  }

  def fromContentType(contentType: Option[String]): Encoder = {
    val colenc = contentType.collect {
      case regex(colenc) => colenc
    }
    fromColenc(colenc)
  }

  def fromColenc(e: Option[String]): Encoder = e match {
    case Base64.colenc => Base64
    case URL.colenc => URL
    case _ => None
  }
}
