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

  def tuneReplication(
      host: Option[String] = None, port: Option[Int] = None, ts: Option[Long] = None, iv: Option[Int] = None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    val params = host.map(("host", _)) :: port.map(("port", _)) :: ts.map(("ts", _)) :: iv.map(("iv", _)) :: Nil
    call("tune_replication", Encoder.None, cp, params.flatten: _*).map(_ => ())
  }

  def status(implicit cp: CommonParams = CommonParams.empty): Try[Status] = {
    call("status", Encoder.None, cp).flatMap { res =>
      Status.extract(parseTsv(res))
    }
  }

  def clear(implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("clear", Encoder.None, cp).map(_ => ())
  }

  def synchronize(hard: Boolean = false, command: Option[String] = None)(
    implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    val params = Seq(if (hard) Some(("hard", "")) else None, command.map(("command", _)))
    call("synchronize", Encoder.None, cp, params.flatten: _*).map(_ => ())
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

  def setBulk(
      records: (String, Array[Byte])*)(
      xt: Option[Long] = None, atomic: Boolean = false, encoder: Encoder = Encoder.None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Int] = {
    val params = toBulkParams(records, atomic, xt)
    call("set_bulk", encoder, cp, params: _*).flatMap { res =>
      val tsv = parseTsv(res).toMap
      Try(tsv("num").toInt)
    }
  }

  def removeBulk(
      keys: String*)(
      atomic: Boolean = false, encoder: Encoder = Encoder.None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Int] = {
    val params = toBulkParams(keys.map((_, "")), atomic)
    call("remove_bulk", encoder, cp, params: _*).flatMap { res =>
      val tsv = parseTsv(res).toMap
      Try(tsv("num").toInt)
    }
  }

  def getBulk(
      keys: String*)(
      atomic: Boolean = false, encoder: Encoder = Encoder.None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Map[String, String]] = {
    val params = toBulkParams(keys.map((_, "")), atomic)
    call("get_bulk", encoder, cp, params: _*) flatMap { res =>
      retrieveArbitraryRecords(parseTsv(res).toMap)(identity)
    }
  }

  def vacuum(
      step: Option[Int] = None, encoder: Encoder = Encoder.None)(
      implicit cp: CommonParams = CommonParams.empty): Try[Unit] = {
    call("vacuum", encoder, cp, step.map(("step", _)).toSeq: _*).map(_ => ())
  }

  def matchPrefix(prefix: String, max: Option[Int] = None, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams.empty): Try[Map[String, Int]] = {
    val params = ("prefix", prefix) +: max.map(("max", _)).toSeq
    call("match_prefix", encoder, cp, params: _*) flatMap { res =>
      retrieveArbitraryRecords(parseTsv(res).toMap)(_.toInt)
    }
  }

  def matchRegex(regex: String, max: Option[Int] = None, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams.empty): Try[Map[String, Int]] = {
    val params = ("regex", regex) +: max.map(("max", _)).toSeq
    call("match_regex", encoder, cp, params: _*) flatMap { res =>
      retrieveArbitraryRecords(parseTsv(res).toMap)(_.toInt)
    }
  }

  def matchSimilar(origin: String, range: Int = 1, utf: Boolean = false, max: Option[Int] = None, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams.empty): Try[Map[String, Int]] = {
    val params = Seq(("origin", origin), ("range", range), ("utf", utf)) ++ max.map(("max", _))
    call("match_similar", encoder, cp, params: _*) flatMap { res =>
      retrieveArbitraryRecords(parseTsv(res).toMap)(_.toInt)
    }
  }

  def curJump(cur: Int, key: Option[String] = None, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[Unit] = {
    checkCurAndCall("cur_jump", cur, encoder, cp).map(_ => ())
  }

  def curJumpBack(cur: Int, key: Option[String] = None, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[Unit] = {
    checkCurAndCall("cur_jump_back", cur, encoder, cp).map(_ => ())
  }

  def curStep(cur: Int, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[Unit] = {
    checkCurAndCall("cur_step", cur, encoder, cp).map(_ => ())
  }

  def curStepBack(cur: Int, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[Unit] = {
    checkCurAndCall("cur_step_back", cur, encoder, cp).map(_ => ())
  }

  def curSetValue(cur: Int, value: String, step: Boolean = false, xt: Option[Long] = None, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[Unit] = {
    val params = (("value", value) +: (if (step) Seq(("step", "")) else Nil)) ++ xt.map(("xt", _))
    checkCurAndCall("cur_set_value", cur, encoder, cp, params: _*).map(_ => ())
  }

  def curRemove(cur: Int, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[Unit] = {
    checkCurAndCall("cur_remove", cur, encoder, cp).map(_ => ())
  }

  def curGetKey(cur: Int, step: Boolean = false, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[String] = {
    val params = if (step) Seq(("step", "")) else Nil
    checkCurAndCall("cur_get_key", cur, encoder, cp, params: _*).flatMap { res =>
      val tsv = parseTsv(res).toMap
      Try(tsv("key"))
    }
  }

  def curGetValue(cur: Int, step: Boolean = false, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[String] = {
    val params = if (step) Seq(("step", "")) else Nil
    checkCurAndCall("cur_get_value", cur, encoder, cp, params: _*) flatMap { res =>
      val tsv = parseTsv(res).toMap
      Try(tsv("value"))
    }
  }

  def curGet(cur: Int, step: Boolean = false, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[(String, String)] = {
    val params = if (step) Seq(("step", "")) else Nil
    checkCurAndCall("cur_get", cur, encoder, cp, params: _*) flatMap { res =>
      val tsv = parseTsv(res).toMap
      for {
        k <- Try(tsv("key"))
        v <- Try(tsv("value"))
      } yield {
        (k, v)
      }
    }
  }

  def curSeize(cur: Int, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[(String, String)] = {
    checkCurAndCall("cur_seize", cur, encoder, cp) flatMap { res =>
      val tsv = parseTsv(res).toMap
      for {
        k <- Try(tsv("key"))
        v <- Try(tsv("value"))
      } yield {
        (k, v)
      }
    }
  }

  def curDelete(cur: Int, encoder: Encoder = Encoder.None)(
    implicit cp: CommonParams = CommonParams(cur = Some(cur))): Try[Unit] = {
    checkCurAndCall("cur_delete", cur, encoder, cp).map(_ => ())
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

  private[this] def toBulkParams(records: Seq[(String, Any)], atomic: Boolean, xt: Option[Long] = None): Seq[(String, Any)] =
    (("atomic", atomic) +: withPrefix(records)) ++ xt.map(("xt", _))

  private[this] def withPrefix[A](records: Seq[(String, A)]): Seq[(String, A)] =
    records.map { case (k, v) => (s"_$k", v) }

  private[this] def retrieveArbitraryRecords[A, B](tsv: Map[String, A])(as: A => B): Try[Map[String, B]] = {
    val records = tsv.collect { case (k, v) if k.startsWith("_") => (k.tail, as(v)) }
    Try {
      val num = tsv.get("num").map(_.toString.toInt).getOrElse(0)
      require(records.size == num)
      records
    }
  }

  private[this] def url(procedure: String): String = {
    s"$baseUrl/rpc/$procedure"
  }

  private[this] def checkCurAndCall(
    procedure: String, cur: Int, encoder: Encoder, cp: CommonParams, params: (String, Any)*): Try[HttpResponse[String]] = Try {
    require(cp.cur.contains(cur))
    call(procedure, encoder, cp, params: _*)
  }.flatten

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
