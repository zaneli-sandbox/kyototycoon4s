package com.zaneli.kyototycoon4s

import com.zaneli.kyototycoon4s.rpc.{CommonParams, Encoder, Status}
import scala.util.{Success, Failure, Try}
import scalaj.http.Http

class RpcClient private[kyototycoon4s] (private[this] val host: String, private[this] val port: Int) {

  private[this] lazy val baseUrl = s"http://$host:$port"

  private[this] case class Response(code: Int, body: String, headers: Map[String, String])

  def void(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("void", Encoder.None, cp).map(_ => ())
  }

  def echo(
      params: (String, Any)*)(
      encoder: Encoder = Encoder.None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Seq[(String, String)]] = {
    call("echo", encoder, cp, params: _*).map(res => parseTsv(res.body))
  }

  def report(implicit cp: CommonParams = CommonParams.empty): Try[Seq[(String, String)]] = {
    call("report", Encoder.None, cp).map(res => parseTsv(res.body))
  }

  def status(implicit cp: CommonParams = CommonParams.empty): Try[Status] = {
    call("status", Encoder.None, cp).flatMap { res =>
      Status.extract(parseTsv(res.body))
    }
  }

  def clear(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("clear", Encoder.None, cp).map(_ => ())
  }

  def set[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Encoder = Encoder.None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("set", key, value, xt, encoder, toBytes, cp)
  }

  def add[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Encoder = Encoder.None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("add", key, value, xt, encoder, toBytes, cp)
  }

  def replace[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Encoder = Encoder.None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("replace", key, value, xt, encoder, toBytes, cp)
  }

  def append[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Encoder = Encoder.None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("append", key, value, xt, encoder, toBytes, cp)
  }

  private[this] def set[A](
      procedure: String, key: String, value: A, xt: Option[Long], encoder: Encoder, toBytes: A => Array[Byte], cp: CommonParams): Try[Unit] = {
    val params = Seq(("key", key), ("value", toBytes(value))) ++ xt.map(("xt", _))
    call(procedure, encoder, cp, params: _*).map(_ => ())
  }

  private[this] def url(procedure: String): String = {
    s"$baseUrl/rpc/$procedure"
  }

  private[this] def call(
      procedure: String, encoder: Encoder, cp: CommonParams, params: (String, Any)*): Try[Response] = {
    val body = toTsv(cp.toParams ++ params, encoder)
    val contentType = "text/tab-separated-values" + encoder.colenc.map(e => s"; colenc=$e").getOrElse("")
    val req = (if (body.nonEmpty) {
      Http(url(procedure)).postData(body)
    } else {
      Http(url(procedure))
    }).header("Content-Type", contentType)
      .timeout(connTimeoutMs = cp.waitTime.getOrElse(5000), readTimeoutMs = cp.waitTime.getOrElse(5000))
    Try(req.asString).flatMap {
      case res if res.isError =>
        Failure(new KyotoTycoonException(res.code, parseTsv(res.body).toMap.get("ERROR")))
      case res => Success(Response(res.code, res.body, res.headers))
    }
  }

  private[this] def toTsv(params: Seq[(String, Any)], encoder: Encoder): String = {
    params.map { case (k, v) =>
      val key = encoder.encode(k)
      val value = v match {
        case bs: Array[Byte] => encoder.encode(bs)
        case x => encoder.encode(x.toString)
      }
      s"$key\t$value"
    }.mkString("\n")
  }

  private[this] def parseTsv(value: String): Seq[(String, String)] = {
    value.lines.flatMap { line =>
      PartialFunction.condOpt(line.split("\t")) {
        case Array(k, v) => (k, v)
      }
    }.toList
  }
}
