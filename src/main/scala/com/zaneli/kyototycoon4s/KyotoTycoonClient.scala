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
  private[this] val charset = "UTF-8"

  def get(key: String): Either[Response, Response] = {
    call(key, "get")
  }

  def head(key: String): Either[Response, Response] = {
    call(key, "head")
  }

  def set(key: String, value: Array[Byte], xt: Option[Int] = None): Either[Response, Response] = {
    put(key, value, "set", xt)
  }

  def add(key: String, value: Array[Byte], xt: Option[Int] = None): Either[Response, Response] = {
    put(key, value, "add", xt)
  }

  def replace(key: String, value: Array[Byte], xt: Option[Int] = None): Either[Response, Response] = {
    put(key, value, "replace", xt)
  }

  def delete(key: String): Either[Response, Response] = {
    call(key, "delete")
  }

  private[this] def put(key: String, value: Array[Byte], mode: String, xt: Option[Int]): Either[Response, Response] = {
    val headers = ("X-Kt-Mode", mode) +: xt.map(x => ("X-Kt-Xt", x.toString)).toSeq
    call(key, "put", value, headers)
  }

  private[this] def url(key: String): String = {
    val path = URLEncoder.encode(key, charset)
    s"$baseUrl/$path"
  }

  private[this] def call(
      key: String,
      method: String,
      body: Array[Byte] = Array.empty[Byte],
      headers: Seq[(String, String)] = Seq.empty[(String, String)]): Either[Response, Response] = {
    val req = Http(url(key)).method(method).timeout(connTimeoutMs = 5000, readTimeoutMs = 5000).headers(headers)
    val res = (if (body.nonEmpty) {
      req.postData(body)
    } else {
      req
    }).asString
    if (res.isError) {
      Left(Response(res.code, res.body, res.headers))
    } else {
      Right(Response(res.code, res.body, res.headers))
    }
  }
}

case class Response(code: Int, body: String, headers: Map[String, String])
