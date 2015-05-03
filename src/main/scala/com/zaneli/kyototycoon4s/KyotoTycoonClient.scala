package com.zaneli.kyototycoon4s

import java.net.URLEncoder
import scalaj.http.Http

object KyotoTycoonClient {
  def rest(host: String, port: Int = 1978): RestClient = {
    new RestClient(host, port)
  }
}

class RestClient(private[this] val host: String, private[this] val port: Int) {

  private[this] val baseUrl = s"http://$host:$port"

  def get(key: String): Either[Response, Response] = {
    call(key, "get")
  }

  def head(key: String): Either[Response, Response] = {
    call(key, "head")
  }

  def set(key: String, value: Value, xt: Option[Int] = None): Either[Response, Response] = {
    put(key, value, "set", xt)
  }

  def add(key: String, value: Value, xt: Option[Int] = None): Either[Response, Response] = {
    put(key, value, "add", xt)
  }

  def replace(key: String, value: Value, xt: Option[Int] = None): Either[Response, Response] = {
    put(key, value, "replace", xt)
  }

  def delete(key: String): Either[Response, Response] = {
    call(key, "delete")
  }

  private[this] def put(key: String, value: Value, mode: String, xt: Option[Int]): Either[Response, Response] = {
    val headers = ("X-Kt-Mode", mode) +: xt.map(x => ("X-Kt-Xt", x.toString)).toSeq
    call(key, "put", value, headers)
  }

  private[this] def url(key: String): String = {
    val path = URLEncoder.encode(key, "UTF-8")
    s"$baseUrl/$path"
  }

  private[this] def call(
      key: String,
      method: String,
      body: Value = Value.empty,
      headers: Seq[(String, String)] = Seq.empty[(String, String)]): Either[Response, Response] = {
    val res = (if (body.content.nonEmpty) {
      Http(url(key)).postData(body.content)
    } else {
      Http(url(key))
    }).method(method).headers(headers).timeout(connTimeoutMs = 5000, readTimeoutMs = 5000).asString
    if (res.isError) {
      Left(Response(res.code, res.body, res.headers))
    } else {
      Right(Response(res.code, res.body, res.headers))
    }
  }
}

case class Response(code: Int, body: String, headers: Map[String, String])

case class Value(content: Array[Byte])

object Value {
  def apply(content: String): Value = {
    Value(content.getBytes("UTF-8"))
  }
  def empty: Value = {
    Value(Array.empty[Byte])
  }
}
