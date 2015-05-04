package com.zaneli

import com.github.nscala_time.time.Imports.DateTime
import java.net.{URLDecoder, URLEncoder}
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

  private[kyototycoon4s] def encode(v: String): String = {
    URLEncoder.encode(v, "UTF-8")
  }

  private[kyototycoon4s] def decode(v: String): String = {
    URLDecoder.decode(v, "UTF-8")
  }
}
