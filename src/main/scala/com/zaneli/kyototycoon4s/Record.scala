package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
import scala.util.Try

case class Record[A](value: A, xt: Option[DateTime])

object Record {

  def apply[A, B](body: A, map: Map[String, B])(toXt: Map[String, B] => Option[DateTime]): Record[A] = {
    Record(body, toXt(map))
  }
}

object Xt {

  def fromHeader(headers: Map[String, String]): Option[DateTime] = {
    headers.get("X-Kt-Xt").flatMap(xt =>
      Try(new DateTime(ZonedDateTime.parse(xt, RFC_1123_DATE_TIME).toInstant.toEpochMilli)).toOption
    )
  }

  def fromTsv[A](tsv: Map[String, Array[Byte]]): Option[DateTime] = {
    for {
      xtBytes <- tsv.get("xt")
      xt <- Try(new String(xtBytes, "UTF-8").toLong).toOption
    } yield {
      new DateTime(xt * 1000)
    }
  }
}
