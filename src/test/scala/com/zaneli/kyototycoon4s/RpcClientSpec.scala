package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import com.zaneli.kyototycoon4s.Implicits._
import com.zaneli.kyototycoon4s.rpc.{Encoder, Origin}
import java.nio.ByteBuffer
import java.util.Arrays
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
      val res = client.echo()()
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.isEmpty)
      }
    }
    it("one param") {
      val res = client.echo(("key", 123))()
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.size === 1)
        assert(params.head === (("key", "123")))
      }
    }
    it("some params") {
      val res = client.echo(("key1", 123), ("key2", "abc"), ("key3", "xyz"))()
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.size === 3)
        assert(params.contains(("key1", "123")))
        assert(params.contains(("key2", "abc")))
        assert(params.contains(("key3", "xyz")))
      }
    }
    it("base64 encode param") {
      val res = client.echo(("a\tb\nc\t", "z\ty\nz\t"))(Encoder.Base64)
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.size === 1)
        assert(params.head === (("a\tb\nc\t", "z\ty\nz\t")))
      }
    }
    it("url encode param") {
      val res = client.echo(("a\tb\nc\t", "z\ty\nz\t"))(Encoder.URL)
      assert(res.isSuccess)
      res.foreach { params =>
        assert(params.size === 1)
        assert(params.head === (("a\tb\nc\t", "z\ty\nz\t")))
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
      assert(client.set(key, value, encoder = Some(Encoder.URL)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("set long value") {
      val key = asKey("test_key_for_set_long")
      val value = 1L
      assert(client.set(key, value).isSuccess)

      val res = Http(restUrl(key)).asBytes
      assert(res.isNotError)
      assert(ByteBuffer.wrap(res.body).getLong === value)
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
      assert(client.add(key, value, encoder = Some(Encoder.URL)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("add long value") {
      val key = asKey("test_key_for_add_long")
      val value = Long.MinValue
      assert(client.add(key, value).isSuccess)

      val res = Http(restUrl(key)).asBytes
      assert(res.isNotError)
      assert(ByteBuffer.wrap(res.body).getLong === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("add value (already key exists)") {
      val key = asKey("test_key_for_add")
      val value = "test_value_for_add"
      prepare(key, "prepared_value")

      val res = client.add(key, value)
      assert(res.isFailure)
      res.failed.foreach(t => assert(t.getMessage == "450: DB: 6: record duplication: record duplication"))
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
      prepare(key, "prepared_value")
      assert(client.replace(key, value, encoder = Some(Encoder.URL)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("replace long value") {
      val key = asKey("test_key_for_replace_long")
      val value = 0L
      prepare(key, "prepared_value")
      assert(client.replace(key, value).isSuccess)

      val res = Http(restUrl(key)).asBytes
      assert(res.isNotError)
      assert(ByteBuffer.wrap(res.body).getLong === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("replace value (key not exists)") {
      val key = asKey("test_key_for_replace")
      val value = "test_value_for_replace"

      val res = client.replace(key, value)
      assert(res.isFailure)
      res.failed.foreach(t => assert(t.getMessage == "450: DB: 7: no record: no record"))
    }
  }

  describe("append") {
    it("append value without xt") {
      val key = asKey("test_key_for_append_without_xt")
      val value = "test_value_for_append_without_xt"
      prepare(key, "prepared_value")
      assert(client.append(key, value).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === "prepared_value" + value)
      assert(getXt(res.headers).isEmpty)
    }
    it("append value with xt") {
      val key = asKey("test_key_for_append_with_xt")
      val value = "test_value_for_append_with_xt"
      prepare(key, "prepared_value")
      val xt = DateTime.now.withMillisOfSecond(0).plusSeconds(30)
      assert(client.append(key, value, Some(30)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === "prepared_value" + value)
      assertWithin(getXt(res.headers), xt)
    }
    it("append value (require url encode)") {
      val key = asKey("te\tst/key\n_for_append?=%~")
      val value = "te\tst/value\n_for_append?=%~"
      prepare(key, "prepared_value")
      assert(client.append(key, value, encoder = Some(Encoder.URL)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === "prepared_value" + value)
      assert(getXt(res.headers).isEmpty)
    }
    it("append value (key not exists)") {
      val key = asKey("test_key_for_append")
      val value = "test_value_for_append"
      assert(client.append(key, value).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
  }

  describe("increment") {
    it("increment value") {
      val key = asKey("test_key_for_increment")
      val value = 10L
      prepare(key, 1L)

      val res = client.increment(key, value)
      assert(res.isSuccess)
      res.foreach(l => assert(l === 11L))
    }
    it("increment value (key not exists)") {
      val key = asKey("test_key_for_increment_key_not_found")
      val value = 1L

      val res = client.increment(key, value)
      assert(res.isSuccess)
      res.foreach(l => assert(l === 1L))
    }
    it("increment value (orig=num)") {
      val key = asKey("test_key_for_increment_orig_num")
      val value = 11L

      val res = client.increment(key, value, orig = Some(Origin.num(22L)))
      assert(res.isSuccess)
      res.foreach(l => assert(l === 33L))
    }
    it("increment value (orig=set)") {
      val key = asKey("test_key_for_increment_orig_set")
      val value = 5L

      val res = client.increment(key, value, orig = Some(Origin.Set))
      assert(res.isSuccess)
      res.foreach(l => assert(l === 5L))
    }
    it("increment value (orig=try)") {
      val key = asKey("test_key_for_increment_orig_try")
      val value = 5L

      val res = client.increment(key, value, orig = Some(Origin.Try))
      assert(res.isFailure)
      res.failed.foreach(t => assert(t.getMessage === "450: DB: 8: logical inconsistency: logical inconsistency"))
    }
  }

  describe("increment_double") {
    it("increment_double value") {
      val key = asKey("test_key_for_increment_double")
      val value = 12.34D
      prepare(key, Array(0, 0, 0, 0, 0, 0, 0, 10, 0, 1, -58, -65, 82, 99, 64, 0))(_.map(_.toByte)) // 10.5D

      val res = client.incrementDouble(key, value)
      assert(res.isSuccess)
      res.foreach(d => assert(d === 22.84D))
    }
    it("increment_double value (key not exists)") {
      val key = asKey("test_key_for_increment_double_key_not_found")
      val value = 55.55D

      val res = client.incrementDouble(key, value)
      assert(res.isSuccess)
      res.foreach(d => assert(d === 55.55D))
    }
    it("increment_double value (orig=num)") {
      val key = asKey("test_key_for_increment_double_orig_num")
      val value = 12.3D

      val res = client.incrementDouble(key, value, orig = Some(Origin.num(0.1D)))
      assert(res.isSuccess)
      res.foreach(d => assert(d === 12.4D))
    }
    it("increment_double value (orig=set)") {
      val key = asKey("test_key_for_increment_double_orig_set")
      val value = 12345.67D

      val res = client.incrementDouble(key, value, orig = Some(Origin.Set))
      assert(res.isSuccess)
      res.foreach(d => assert(d === 12345.67D))
    }
    it("increment_double value (orig=try)") {
      val key = asKey("test_key_for_increment_double_orig_try")
      val value = 12345.67D

      val res = client.incrementDouble(key, value, orig = Some(Origin.Try))
      assert(res.isFailure)
      res.failed.foreach(t => assert(t.getMessage === "450: DB: 8: logical inconsistency: logical inconsistency"))
    }
  }

  describe("remove") {
    it("remove value") {
      val key = asKey("test_key_for_remove")
      prepare(key, "prepared_value")
      val res = client.remove(key)
      assert(res.isSuccess)
    }
    it("remove value (key not exists)") {
      val key = asKey("test_key_for_remove_key_not_found")
      val res = client.remove(key)
      assert(res.isFailure)
      res.failed.foreach(t => assert(t.getMessage === "450: DB: 7: no record: no record"))
    }
  }

  describe("getString") {
    it("value exists") {
      val key = asKey("test_key_for_get_string")
      val value = "test_value_for_get_string"
      prepare(key, value)

      val res = client.getString(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.contains(value))
        assert(x.isEmpty)
      }
    }
    it("value with xt exists") {
      val key = asKey("test_key_for_get_string_with_xt")
      val value = "test_value_for_get_string_with_xt"
      val xt = DateTime.now.plusMinutes(10)
      prepare(key, value, Some(xt.getMillis / 1000))

      val res = client.getString(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.contains(value))
        assert(x.exists(_.getMillis == xt.withMillisOfSecond(0).getMillis))
      }
    }
    it("value exists (key require url encode)") {
      val key = asKey("te\tst/key_for_get_string\n?=%~")
      val value = "te\tst/key_for_get_string\n?=%~"
      prepare(key, value)
      val res = client.getString(key, encoder = Encoder.URL)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.contains(value))
        assert(x.isEmpty)
      }
    }
    it("value not exists") {
      val res = client.getString("test_key_for_get_string_not_found")
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "450: DB: 7: no record: no record")
    }
  }

  describe("getBytes") {
    it("value exists") {
      val key = asKey("test_key_for_get_bytes")
      val value = "test_value_for_get_bytes"
      prepare(key, value)

      val res = client.getBytes(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.exists(Arrays.equals(_, value.getBytes("UTF-8"))))
        assert(x.isEmpty)
      }
    }
    it("value with xt exists") {
      val key = asKey("test_key_for_get_bytes_with_xt")
      val value = "test_value_for_get_bytes_with_xt"
      val xt = DateTime.now.plusMinutes(10)
      prepare(key, value, Some(xt.getMillis / 1000))

      val res = client.getBytes(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.exists(Arrays.equals(_, value.getBytes("UTF-8"))))
        assert(x.exists(_.getMillis == xt.withMillisOfSecond(0).getMillis))
      }
    }
    it("value exists (key require url encode)") {
      val key = asKey("te\tst/key_for_get_bytes\n?=%~")
      val value = "te\tst/key_for_get_bytes\n?=%~"
      prepare(key, value)
      val res = client.getBytes(key, encoder = Encoder.URL)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.exists(Arrays.equals(_, value.getBytes("UTF-8"))))
        assert(x.isEmpty)
      }
    }
    it("value not exists") {
      val res = client.getBytes("test_key_for_get_bytes_not_found")
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "450: DB: 7: no record: no record")
    }
  }

  describe("getLong") {
    it("value exists") {
      val key = asKey("test_key_for_get_long")
      val value = Long.MaxValue
      prepare(key, value)

      val res = client.getLong(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.contains(value))
        assert(x.isEmpty)
      }
    }
    it("value with xt exists") {
      val key = asKey("test_key_for_get_long_with_xt")
      val value = 10L
      val xt = DateTime.now.plusMinutes(10)
      prepare(key, value, Some(xt.getMillis / 1000))

      val res = client.getLong(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.contains(value))
        assert(x.exists(_.getMillis == xt.withMillisOfSecond(0).getMillis))
      }
    }
    it("value exists (key require url encode)") {
      val key = asKey("te\tst/key_for_get_long\n?=%~")
      val value = 0L
      prepare(key, value)
      val res = client.getLong(key, encoder = Encoder.URL)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v.contains(value))
        assert(x.isEmpty)
      }
    }
    it("value not exists") {
      val res = client.getLong("test_key_for_get_long_not_found")
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
