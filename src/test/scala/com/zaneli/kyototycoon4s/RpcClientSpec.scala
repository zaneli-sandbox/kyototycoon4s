package com.zaneli.kyototycoon4s

import org.scalatest.FunSpec

class RpcClientSpec extends FunSpec {

  private[this] val (host, port) = ("localhost", 1978)

  private[this] val client = KyotoTycoonClient.rpc(host, port)

  describe("void") {
    it("no params") {
      val res = client.void
      assert(res.isSuccess)
    }
  }

  describe("echo") {
    it("no params") {
      val res = client.echo()
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.isEmpty)
      }
    }
    it("one param") {
      val res = client.echo(("key", 123))
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.size === 1)
        assert(params.head === (("key", "123")))
      }
    }
    it("some params") {
      val res = client.echo(("key1", 123), ("key2", "abc"), ("key3", "xyz"))
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.size === 3)
        assert(params.contains(("key1", "123")))
        assert(params.contains(("key2", "abc")))
        assert(params.contains(("key3", "xyz")))
      }
    }
  }

  describe("report") {
    it("no params") {
      val res = client.report
      assert(res.isSuccess)
    }
  }

  private[this] def url(procedure: String): String = {
    s"http://$host:$port/rpc/$procedure"
  }
}
