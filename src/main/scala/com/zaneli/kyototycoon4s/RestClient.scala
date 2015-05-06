package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import java.nio.ByteBuffer
import org.apache.commons.codec.net.URLCodec
import scala.util.{Success, Failure, Try}
import scalaj.http.{Http, HttpResponse, HttpRequest}

class RestClient private[kyototycoon4s] (private[this] val host: String, private[this] val port: Int) {

  private[this] lazy val baseUrl = s"http://$host:$port"

  private[this] lazy val codec = new URLCodec()

  def getString(key: String): Try[Record[String]] = {
    call(_.asString)(key, "get").map { case (body, headers) => Record(body, headers)(Xt.fromHeader) }
  }

  def getBytes(key: String): Try[Record[Array[Byte]]] = {
    call(_.asBytes)(key, "get").map { case (body, headers) => Record(body, headers)(Xt.fromHeader) }
  }

  def getLong(key: String): Try[Record[Long]] = {
    getBytes(key).map(r => Record(ByteBuffer.wrap(r.value).getLong, r.xt))
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
    val path = codec.encode(key)
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
