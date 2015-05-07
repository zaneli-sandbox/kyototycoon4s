package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import com.zaneli.kyototycoon4s.rpc.{CommonParams, Encoder, Origin, Status}
import scala.util.{Success, Failure, Try}
import scalaj.http.{Http, HttpResponse}

class RpcClient private[kyototycoon4s] (private[this] val host: String, private[this] val port: Int) {

  private[this] lazy val baseUrl = s"http://$host:$port"

  def void(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("void", Encoder.None, cp).map(_ => ())
  }

  def echo(
      params: (String, Any)*)(
      encoder: Encoder = Encoder.None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Seq[(String, String)]] = {
    call("echo", encoder, cp, params: _*).map(res => parseTsv(res))
  }

  def report(implicit cp: CommonParams = CommonParams.empty): Try[Seq[(String, String)]] = {
    call("report", Encoder.None, cp).map(res => parseTsv(res))
  }

  def status(implicit cp: CommonParams = CommonParams.empty): Try[Status] = {
    call("status", Encoder.None, cp).flatMap { res =>
      Status.extract(parseTsv(res))
    }
  }

  def clear(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("clear", Encoder.None, cp).map(_ => ())
  }

  def set[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Option[Encoder] = None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("set", key, value, xt, encoder, toBytes, cp)
  }

  def add[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Option[Encoder] = None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("add", key, value, xt, encoder, toBytes, cp)
  }

  def replace[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Option[Encoder] = None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("replace", key, value, xt, encoder, toBytes, cp)
  }

  def append[A](
      key: String, value: A, xt: Option[Long] = None, encoder: Option[Encoder] = None)(
      implicit toBytes: A => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    set("append", key, value, xt, encoder, toBytes, cp)
  }

  def increment(
      key: String, num: Long, orig: Option[Origin[Long]] = None, xt: Option[Long] = None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Long] = {
    increment("increment", key, num, orig, xt, cp)(_.toLong)
  }

  def incrementDouble(
      key: String, num: Double, orig: Option[Origin[Double]] = None, xt: Option[Long] = None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Double] = {
    increment("increment_double", key, num, orig, xt, cp)(_.toDouble)
  }

  def cas[A, B](
      key: String,
      oval: Option[A] = Option.empty[String],
      nval: Option[B] = Option.empty[String],
      xt: Option[Long] = None,
      encoder: Option[Encoder] = None)(
      implicit oToBytes: A => Array[Byte], nToBytes: B => Array[Byte], cp: CommonParams = CommonParams.empty): Try[Unit] = {
    val params = Seq(("key", key)) ++ oval.map(o => ("oval", oToBytes(o))) ++ nval.map(n => ("nval", nToBytes(n))) ++ xt.map(("xt", _))
    val e = (encoder, Seq(oval, nval).flatten) match {
      case (Some(enc), _) => enc
      case (_, vs) if vs.forall(_.isInstanceOf[String]) => Encoder.None
      case _ => Encoder.Base64
    }
    call("cas", e, cp, params: _*).map(_ => ())
  }

  def remove(key: String, encoder: Encoder = Encoder.None)(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("remove", encoder, cp, ("key", key)).map(_ => ())
  }

  def get[A](
      key: String, encoder: Encoder = Encoder.None, as: Array[Byte] => A = { bs: Array[Byte] => new String(bs, "UTF-8") })(
      implicit cp: CommonParams = CommonParams.empty): Try[Record[A]] = {
    getValue("get", key, encoder, cp)(as)
  }

  def check(
      key: String, encoder: Encoder = Encoder.None)(
      implicit cp: CommonParams = CommonParams.empty): Try[(Long, Option[DateTime])] = {
    call("check", encoder, cp, ("key", key)).map { res =>
      val tsv = parseTsv(res).toMap
      val vsiz = tsv.get("vsiz").flatMap(s => Try(s.toLong).toOption).getOrElse(0L)
      (vsiz, Xt.fromTsv(tsv)(identity))
    }
  }

  def seize[A](
      key: String, encoder: Encoder = Encoder.None, as: Array[Byte] => A = { bs: Array[Byte] => new String(bs, "UTF-8") })(
      implicit cp: CommonParams = CommonParams.empty): Try[Record[A]] = {
    getValue("seize", key, encoder, cp)(as)
  }

  private[this] def set[A](
      procedure: String, key: String, value: A, xt: Option[Long], encoder: Option[Encoder], toBytes: A => Array[Byte], cp: CommonParams): Try[Unit] = {
    val params = Seq(("key", key), ("value", toBytes(value))) ++ xt.map(("xt", _))
    val e = (encoder, value) match {
      case (Some(enc), _) => enc
      case (_, _: String) => Encoder.None
      case _ => Encoder.Base64
    }
    call(procedure, e, cp, params: _*).map(_ => ())
  }

  private[this] def increment[A](
      procedure: String, key: String, num: A, orig: Option[Origin[A]], xt: Option[Long], cp: CommonParams)(
      f: String => A): Try[A] = {
    val params = Seq(("key", key), ("num", num)) ++ orig.map(o => ("orig", o.value)) ++ xt.map(("xt", _))
    for {
      res <- call(procedure, Encoder.None, cp, params: _*)
      tsv = parseTsv(res).toMap
      numStr <- Try(tsv("num"))
      num <- Try(f(numStr))
    } yield {
      num
    }
  }

  private[this] def getValue[A](
      procedure: String, key: String, encoder: Encoder, cp: CommonParams)(as: Array[Byte] => A): Try[Record[A]] = {
    for {
      res <- call(procedure, encoder, cp, ("key", key))
      tsv = parseTsv(res, _.decode(_)).toMap
      value <- Try(tsv("value"))
    } yield {
      Record(as(value), Xt.fromTsv(tsv)(new String(_, "UTF-8")))
    }
  }

  private[this] def url(procedure: String): String = {
    s"$baseUrl/rpc/$procedure"
  }

  private[this] def call(
      procedure: String, encoder: Encoder, cp: CommonParams, params: (String, Any)*): Try[HttpResponse[String]] = {
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
        Failure(new KyotoTycoonException(res.code, parseTsv(res).toMap.get("ERROR")))
      case res => Success(res)
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

  private[this] def parseTsv[A](
      res: HttpResponse[String],
      decodeValue: (Encoder, String) => A = { (e: Encoder, s: String) => e.decodeString(s) }): Seq[(String, A)] = {
    val encoder = Encoder.fromContentType(res.contentType)
    res.body.lines.flatMap { line =>
      PartialFunction.condOpt(line.split("\t")) {
        case Array(k, v) => (encoder.decodeString(k), decodeValue(encoder, v))
      }
    }.toList
  }
}
