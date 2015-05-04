package com.zaneli.kyototycoon4s

import org.scalatest.FunSpec
import scalaj.http.Http

class RpcClientSpec extends FunSpec with ClientSpecBase {

  override protected[this] val (host, port) = ("localhost", 1978)

  private[this] val client = KyotoTycoonClient.rpc(host, port)

  describe("void") {
    it("no params") {
      val res = client.void()
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
      val res = client.report()
      assert(res.isSuccess)
    }
  }

  describe("status") {
    it("no params") {
      val res = client.status()
      assert(res.isSuccess)
      res.foreach { status =>
        assert(status.count >= 0)
        assert(status.size >= 0)
        assert(status.params.nonEmpty)
      }
    }
  }

  describe("clear") {
    it("no params") {
      val key1 = asKey("key1_for_clear")
      val key2 = asKey("key2_for_clear")
      prepare(key1, "value1")
      prepare(key2, "value2")
      assert(Http(restUrl(key1)).asString.isNotError)
      assert(Http(restUrl(key2)).asString.isNotError)

      val res = client.clear()
      assert(res.isSuccess)

      assert(Http(restUrl(key1)).asString.code === 404)
      assert(Http(restUrl(key2)).asString.code === 404)
    }
  }

  private[this] def rpcUrl(procedure: String): String = {
    s"http://$host:$port/rpc/$procedure"
  }
}
