package com.zaneli.kyototycoon4s.rpc

import scala.util.Try

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
