package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import java.net.URLEncoder
import scala.util.{Failure, Success, Try}
import scalaj.http.{Http, HttpRequest, HttpResponse}

object KyotoTycoonClient {
  def rest(host: String, port: Int = 1978): RestClient = {
    new RestClient(host, port)
  }
}

class RestClient(private[this] val host: String, private[this] val port: Int) {

  private[this] val baseUrl = s"http://$host:$port"

  def getString(key: String): Try[(String, Option[DateTime])] = {
    call(_.asString)(key, "get").map(res => (res.body, getXt(res.headers)))
  }

  def getBytes(key: String): Try[(Array[Byte], Option[DateTime])] = {
    call(_.asBytes)(key, "get").map(res => (res.body, getXt(res.headers)))
  }

  def head(key: String): Try[(Long, Option[DateTime])] = {
    for {
      res <- call(_.asString)(key, "head")
      length <- Try(res.headers.get("Content-Length").get.toLong)
    } yield {
      (length, getXt(res.headers))
    }
  }

  def set(key: String, value: Value, xt: Option[DateTime] = None): Try[Unit] = {
    put(key, value, "set", xt).map(_ => ())
  }

  def add(key: String, value: Value, xt: Option[DateTime] = None): Try[Unit] = {
    put(key, value, "add", xt).map(_ => ())
  }

  def replace(key: String, value: Value, xt: Option[DateTime] = None): Try[Unit] = {
    put(key, value, "replace", xt).map(_ => ())
  }

  def delete(key: String): Try[Unit] = {
    call(_.asString)(key, "delete").map(_ => ())
  }

  private[this] def put(key: String, value: Value, mode: String, xt: Option[DateTime]): Try[Response[String]] = {
    val headers = ("X-Kt-Mode", mode) +: xt.map(x => ("X-Kt-Xt", (x.getMillis / 1000).toString)).toSeq
    call(_.asString)(key, "put", value, headers)
  }

  private[this] def url(key: String): String = {
    val path = URLEncoder.encode(key, "UTF-8")
    s"$baseUrl/$path"
  }

  private[this] def call[A](
      as: (HttpRequest => HttpResponse[A]))(
      key: String,
      method: String,
      body: Value = Value.empty,
      headers: Seq[(String, String)] = Seq.empty[(String, String)]): Try[Response[A]] = {
    val req = (if (body.content.nonEmpty) {
      Http(url(key)).postData(body.content)
    } else {
      Http(url(key))
    }).method(method).headers(headers).timeout(connTimeoutMs = 5000, readTimeoutMs = 5000)
    Try(as(req)).flatMap {
      case res if res.isError => Failure(new KyotoTycoonException(res.code, getError(res.headers)))
      case res => Success(Response(res.code, res.body, res.headers))
    }
  }
}

private[kyototycoon4s] case class Response[A](code: Int, body: A, headers: Map[String, String])

class KyotoTycoonException(code: Int, error: Option[String]) extends Exception(s"$code: ${error.getOrElse("")}")

case class Value(content: Array[Byte])

object Value {
  def apply(content: String): Value = {
    Value(content.getBytes("UTF-8"))
  }
  def empty: Value = {
    Value(Array.empty[Byte])
  }
}
