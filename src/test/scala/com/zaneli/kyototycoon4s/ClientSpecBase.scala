package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import org.apache.commons.codec.net.URLCodec
import org.scalatest.{BeforeAndAfter, Suite}
import scala.collection.mutable.Set
import scala.util.Try
import scalaj.http.Http

trait ClientSpecBase extends BeforeAndAfter { this: Suite =>

  protected[this] val keys: Set[String] = Set()

  protected[this] def host: String
  protected[this] def port: Int

  after {
    keys.foreach { key =>
      Try(Http(restUrl(key)).method("delete").asString)
    }
    keys.clear()
  }

  protected[this] def asKey(key: String): String = {
    keys.add(key)
    key
  }

  protected[this] def prepare[A](
      key: String, value: A, xt: Option[Long] = None)(implicit f: A => Array[Byte]): Unit = {
    val req = Http(restUrl(key)).postData(f(value)).method("put")
    xt.fold(req)(x => req.header("X-Kt-Xt", x.toString)).asString
  }

  protected[this] def getXt(headers: Map[String, String]): Option[DateTime] = {
    Xt.fromHeader(headers)
  }

  protected[this] def restUrl(key: String): String = {
    s"http://$host:$port/${urlEncode(key)}"
  }

  private[this] def urlEncode(s: String): String = new URLCodec().encode(s)
}
