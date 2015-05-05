package com.zaneli

import com.github.nscala_time.time.Imports.DateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME
import scala.util.Try

package object kyototycoon4s {

  private[kyototycoon4s] def getXt(headers: Map[String, String]): Option[DateTime] = {
    headers.get("X-Kt-Xt").flatMap(xt =>
      Try(new DateTime(ZonedDateTime.parse(xt, RFC_1123_DATE_TIME).toInstant.toEpochMilli)).toOption
    )
  }

  private[kyototycoon4s] def getError(headers: Map[String, String]): Option[String] = {
    headers.get("X-Kt-Error")
  }
}
