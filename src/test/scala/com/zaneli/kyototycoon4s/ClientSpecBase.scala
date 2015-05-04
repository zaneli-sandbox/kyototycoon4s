package com.zaneli.kyototycoon4s

import java.net.URLEncoder
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
      Try(Http(restUrl(URLEncoder.encode(key, "UTF-8"))).method("delete").asString)
    }
    keys.clear()
  }

  protected[this] def asKey(key: String): String = {
    keys.add(key)
    key
  }

  protected[this] def prepare(key: String, value: String, xt: Option[Long] = None): Unit = {
    val req = Http(restUrl(key)).postData(value).method("put")
    xt.fold(req)(x => req.header("X-Kt-Xt", x.toString)).asString
  }

  protected[this] def restUrl(key: String): String = {
    s"http://$host:$port/$key"
  }
}
