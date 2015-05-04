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
    (call("echo", cp, params: _*).map(res => parseTsv(res.body)))
  }

  private[this] def url(procedure: String): String = {
    s"$baseUrl/rpc/$procedure"
  }

  private[this] def call(
      procedure: String, cp: CommonParams, params: (String, Any)*): Try[Response] = {
    val body = toTsv(cp.toParams ++ params.map { case (k, v) => (k, v.toString) })
    val req = (if (body.nonEmpty) {
      Http(url(procedure)).postData(body)
    } else {
      Http(url(procedure))
    }).method("get").header("Content-Type", "text/tab-separated-values")
      .timeout(connTimeoutMs = cp.waitTime.getOrElse(5000), readTimeoutMs = cp.waitTime.getOrElse(5000))
    Try(req.asString).flatMap {
      case res if res.isError =>
        val message = PartialFunction.condOpt(Option(res.body)) {
          case s@Some(x) if x.nonEmpty => s
        }.flatten
        Failure(new KyotoTycoonException(res.code, message))
      case res => Success(Response(res.code, res.body, res.headers))
    }
  }

  private[this] def toTsv(params: Seq[(String, String)]): String = {
    params.map { case (k, v) => s"$k\t$v"}.mkString("\n")
  }
  private[this] def parseTsv(value: String): Seq[(String, String)] = {
    value.lines.flatMap { line =>
      line.split("\t") match {
        case Array(k, v) => Some((k, v))
        case _ => None
      }
    }.toList
  }
}

case class CommonParams(
    db: Option[String] = None,
    cur: Option[String] = None,
    waitKey: Option[String] = None,
    waitTime: Option[Int] = None) {

  def toParams: Seq[(String, String)] = {
    val params = db.map(("DB", _)) ::
      cur.map(c => ("CUR", c.toString)) ::
      waitKey.map(("WAIT", _)) ::
      waitTime.map(t => ("WAITTIME", t.toString)) :: Nil
    params.flatten
  }
}

object CommonParams {
  lazy val empty: CommonParams = CommonParams()
}
