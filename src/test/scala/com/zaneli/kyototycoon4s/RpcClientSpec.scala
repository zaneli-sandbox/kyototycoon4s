package com.zaneli.kyototycoon4s

import org.scalatest.FunSpec

class RpcClientSpec extends FunSpec {

  private[this] val (host, port) = ("localhost", 1978)

  private[this] val client = KyotoTycoonClient.rpc(host, port)

  describe("void") {
    it("no params") {
      val res = client.void()
      assert(res.isSuccess)
    }
  }

  private[this] def url(procedure: String): String = {
    s"http://$host:$port/rpc/$procedure"
  }
}
