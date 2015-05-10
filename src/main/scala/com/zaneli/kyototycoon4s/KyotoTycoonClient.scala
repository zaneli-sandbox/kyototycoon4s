package com.zaneli.kyototycoon4s

object KyotoTycoonClient {

  def binary(host: String, port: Int = 1978): BinaryClient = {
    new BinaryClient(host, port)
  }

  def rest(host: String, port: Int = 1978): RestClient = {
    new RestClient(host, port)
  }

  def rpc(host: String, port: Int = 1978): RpcClient = {
    new RpcClient(host, port)
  }
}

class KyotoTycoonException(code: Int, message: Option[String]) extends Exception(s"$code: ${message.getOrElse("")}")
