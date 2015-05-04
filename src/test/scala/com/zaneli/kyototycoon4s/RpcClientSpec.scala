package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import org.scalatest.FunSpec
import scala.math.abs
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

  describe("set") {
    it("set value without xt") {
      val key = asKey("test_key_for_set_without_xt")
      val value = "test_value_for_set_without_xt"
      assert(client.set(key, value).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("set value with xt") {
      val key = asKey("test_key_for_set_with_xt")
      val value = "test_value_for_set_with_xt"
      val xt = DateTime.now.withMillisOfSecond(0).plusSeconds(30)
      assert(client.set(key, value, Some(30)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assertWithin(getXt(res.headers), xt)
    }
    it("set value (require url encode)") {
      val key = asKey("te\tst/key\n_for_set?=%~")
      val value = "te\tst/value\n_for_set?=%~"
      assert(client.set(key, value).isSuccess)

      val res = Http(restUrl(encode(key))).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
  }

  describe("add") {
    it("add value without xt") {
      val key = asKey("test_key_for_add_without_xt")
      val value = "test_value_for_add_without_xt"
      assert(client.add(key, value).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("add value with xt") {
      val key = asKey("test_key_for_add_with_xt")
      val value = "test_value_for_add_with_xt"
      val xt = DateTime.now.withMillisOfSecond(0).plusSeconds(30)
      assert(client.add(key, value, Some(30)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assertWithin(getXt(res.headers), xt)
    }
    it("add value (require url encode)") {
      val key = asKey("te\tst/key\n_for_add?=%~")
      val value = "te\tst/value\n_for_add?=%~"
      assert(client.add(key, value).isSuccess)

      val res = Http(restUrl(encode(key))).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("add value (already key exists)") {
      val key = asKey("test_key_for_add")
      val value = "test_value_for_add"
      prepare(key, "prepared_value")

      val res = client.add(key, value)
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "450: DB: 6: record duplication: record duplication")
    }
  }

  describe("replace") {
    it("replace value without xt") {
      val key = asKey("test_key_for_replace_without_xt")
      val value = "test_value_for_replace_without_xt"
      prepare(key, "prepared_value")
      assert(client.replace(key, value).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("replace value with xt") {
      val key = asKey("test_key_for_replace_with_xt")
      val value = "test_value_for_replace_with_xt"
      prepare(key, "prepared_value")
      val xt = DateTime.now.withMillisOfSecond(0).plusSeconds(30)
      assert(client.replace(key, value, Some(30)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assertWithin(getXt(res.headers), xt)
    }
    it("replace value (require url encode)") {
      val key = asKey("te\tst/key\n_for_replace?=%~")
      val value = "te\tst/value\n_for_replace?=%~"
      prepare(encode(key), "prepared_value")
      assert(client.replace(key, value).isSuccess)

      val res = Http(restUrl(encode(key))).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("replace value (key not exists)") {
      val key = asKey("test_key_for_replace")
      val value = "test_value_for_replace"

      val res = client.replace(key, value)
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "450: DB: 7: no record: no record")
    }
  }

  private[this] def assertWithin(actual: Option[DateTime], expected: DateTime, ms: Long = 1000L): Unit = {
    assert(actual.map(_.getMillis - expected.getMillis).exists(abs(_) <= ms))
  }

  private[this] def rpcUrl(procedure: String): String = {
    s"http://$host:$port/rpc/$procedure"
  }
}
