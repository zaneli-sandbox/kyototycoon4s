package com.zaneli.kyototycoon4s

import scala.util.{Success, Failure, Try}
import scalaj.http.Http

class RpcClient private[kyototycoon4s] (private[this] val host: String, private[this] val port: Int) {

  private[this] lazy val baseUrl = s"http://$host:$port"

  private[this] case class Response(code: Int, body: String, headers: Map[String, String])

  def void(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("void", cp).map(_ => ())
  }

  def echo(params: (String, Any)*)(implicit cp: CommonParams = CommonParams.empty): Try[Seq[(String, String)]] = {
    call("echo", cp, params: _*).map(res => parseTsv(res.body))
  }

  def report(implicit cp: CommonParams = CommonParams.empty): Try[Seq[(String, String)]] = {
    call("report", cp).map(res => parseTsv(res.body))
  }

  def status(implicit cp: CommonParams = CommonParams.empty): Try[Status] = {
    call("status", cp).flatMap { res =>
      Status.extract(parseTsv(res.body))
    }
  }

  def clear(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("clear", cp).map(_ => ())
  }

  def set(
      key: String, value: String, xt: Option[Long] = None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("set", key, value, xt, cp)
  }

  def add(
      key: String, value: String, xt: Option[Long] = None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("add", key, value, xt, cp)
  }

  def replace(
      key: String, value: String, xt: Option[Long] = None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("replace", key, value, xt, cp)
  }

  def append(
      key: String, value: String, xt: Option[Long] = None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("append", key, value, xt, cp)
  }

  private[this] def set(
      procedure: String, key: String, value: String, xt: Option[Long], cp: CommonParams): Try[Unit] = {
    val params = Seq(("key", key), ("value", value)) ++ xt.map(("xt", _))
    call(procedure, cp, params: _*).map(_ => ())
  }

  private[this] def url(procedure: String): String = {
    s"$baseUrl/rpc/$procedure"
  }

  private[this] def call(
      procedure: String, cp: CommonParams, params: (String, Any)*): Try[Response] = {
    val body = toTsv(cp.toParams ++ params)
    val req = (if (body.nonEmpty) {
      Http(url(procedure)).postData(body)
    } else {
      Http(url(procedure))
    }).header("Content-Type", "text/tab-separated-values; colenc=U")
      .timeout(connTimeoutMs = cp.waitTime.getOrElse(5000), readTimeoutMs = cp.waitTime.getOrElse(5000))
    Try(req.asString).flatMap {
      case res if res.isError =>
        Failure(new KyotoTycoonException(res.code, parseTsv(res.body).toMap.get("ERROR")))
      case res => Success(Response(res.code, res.body, res.headers))
    }
  }

  private[this] def toTsv(params: Seq[(String, Any)]): String = {
    params.map { case (k, v) =>
      val key = encode(k)
      val value = encode(v.toString)
      s"$key\t$value"
    }.mkString("\n")
  }

  private[this] def parseTsv(value: String): Seq[(String, String)] = {
    value.lines.flatMap { line =>
      PartialFunction.condOpt(line.split("\t")) {
        case Array(k, v) => (decode(k), decode(v))
      }
    }.toList
  }
}

case class CommonParams(
    db: Option[String] = None,
    cur: Option[String] = None,
    waitKey: Option[String] = None,
    waitTime: Option[Int] = None) {

  def toParams: Seq[(String, Any)] = {
    val params = db.map(("DB", _)) ::
      cur.map(("CUR", _)) ::
      waitKey.map(("WAIT", _)) ::
      waitTime.map(("WAITTIME", _)) :: Nil
    params.flatten
  }
}

object CommonParams {
  lazy val empty: CommonParams = CommonParams()
}

case class Status(count: Int, size: Long, params: Seq[(String, String)])

object Status {
  def extract(params: Seq[(String, String)]): Try[Status] = {
    val map = params.toMap
    for {
      count <- Try(map("count").toInt)
      size <- Try(map("size").toLong)
    } yield {
      Status(count, size, map.filter { case (k, _) => k != "count" && k != "size" }.toList)
    }
  }
}
