package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import scala.util.{Failure, Success, Try}
import scalaj.http.{Http, HttpConstants, HttpRequest, HttpResponse}

class RestClient private[kyototycoon4s] (private[this] val host: String, private[this] val port: Int) {

  private[this] lazy val baseUrl = s"http://$host:$port"

  def get[A](key: String, as: Array[Byte] => A = { bs: Array[Byte] => new String(bs, "UTF-8") }): Try[Record[A]] = {
    call(_.asBytes)(key, "get").map { case (body, headers) => Record(as(body), headers)(Xt.fromHeader) }
  }

  def head(key: String): Try[(Long, Option[DateTime])] = {
    for {
      (body, headers) <- call(_.asString)(key, "head")
      length <- Try(headers.get("Content-Length").get.toLong)
    } yield {
      (length, Xt.fromHeader(headers))
    }
  }

  def set[A](key: String, value: A, xt: Option[DateTime] = None)(implicit toBytes: A => Array[Byte]): Try[Unit] = {
    put(key, value, "set", xt, toBytes).map(_ => ())
  }

  def add[A](key: String, value: A, xt: Option[DateTime] = None)(implicit toBytes: A => Array[Byte]): Try[Unit] = {
    put(key, value, "add", xt, toBytes).map(_ => ())
  }

  def replace[A](key: String, value: A, xt: Option[DateTime] = None)(implicit toBytes: A => Array[Byte]): Try[Unit] = {
    put(key, value, "replace", xt, toBytes).map(_ => ())
  }

  def delete(key: String): Try[Unit] = {
    call(_.asString)(key, "delete").map(_ => ())
  }

  private[this] def put[A](key: String, value: A, mode: String, xt: Option[DateTime], toBytes: A => Array[Byte]): Try[Unit] = {
    val headers = ("X-Kt-Mode", mode) +: xt.map(x => ("X-Kt-Xt", (x.getMillis / 1000).toString)).toSeq
    call(_.asBytes)(key, "put", toBytes(value), headers).map(_ => ())
  }

  private[this] def url(key: String): String = {
    val path = HttpConstants.urlEncode(key, "UTF-8")
    s"$baseUrl/$path"
  }

  private[this] def call[A](
      as: (HttpRequest => HttpResponse[A]))(
      key: String,
      method: String,
      body: Array[Byte] = Array.empty[Byte],
      headers: Seq[(String, String)] = Seq.empty[(String, String)]): Try[(A, Map[String, String])] = {
    val req = (if (body.nonEmpty) {
      Http(url(key)).postData(body)
    } else {
      Http(url(key))
    }).method(method).headers(headers).timeout(connTimeoutMs = 5000, readTimeoutMs = 5000)
    Try(as(req)).flatMap {
      case res if res.isError => Failure(new KyotoTycoonException(res.code, getError(res.headers)))
      case res => Success((res.body, res.headers))
    }
  }

  private[this] def getError(headers: Map[String, String]): Option[String] = {
    headers.get("X-Kt-Error")
  }
}
