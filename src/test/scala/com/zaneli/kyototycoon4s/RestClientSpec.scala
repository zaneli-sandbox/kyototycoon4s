package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import java.util.Arrays
import org.scalatest.FunSpec
import scalaj.http.Http

class RestClientSpec extends FunSpec with ClientSpecBase {

  override protected[this] val (host, port) = ("localhost", 1978)

  private[this] val client = KyotoTycoonClient.rest(host, port)

  describe("getString") {
    it("value exists") {
      val key = asKey("test_key")
      val value = "test_value"
      prepare(key, value)
      val res = client.getString(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v === value)
        assert(x.isEmpty)
      }
    }
    it("value with xt exists") {
      val key = asKey("test_key_xt")
      val value = "test_value_xt"
      val xt = DateTime.now.plusMinutes(10)
      prepare(key, value, Some(xt.getMillis / 1000))
      val res = client.getString(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v === value)
        assert(x.exists(_.getMillis == xt.withMillisOfSecond(0).getMillis))
      }
    }
    it("value exists (key require url encode)") {
      val key = asKey("te st/key?=%~")
      val value = "te st/value?=%~"
      prepare(encode(key), value)
      val res = client.getString(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(v === value)
        assert(x.isEmpty)
      }
    }
    it("value not exists") {
      val res = client.getString("not_found")
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "404: DB: 7: no record: no record")
    }
  }

  describe("getBytes") {
    it("value exists") {
      val key = asKey("test_key")
      val value = "test_value"
      prepare(key, value)
      val res = client.getBytes(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(Arrays.equals(v, value.getBytes("UTF-8")))
        assert(x.isEmpty)
      }
    }
    it("value with xt exists") {
      val key = asKey("test_key_xt")
      val value = "test_value_xt"
      val xt = DateTime.now.plusMinutes(10)
      prepare(key, value, Some(xt.getMillis / 1000))
      val res = client.getBytes(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(Arrays.equals(v, value.getBytes("UTF-8")))
        assert(x.exists(_.getMillis == xt.withMillisOfSecond(0).getMillis))
      }
    }
    it("value exists (key require url encode)") {
      val key = asKey("te st/key?=%~")
      val value = "te st/value?=%~"
      prepare(encode(key), value)
      val res = client.getBytes(key)
      assert(res.isSuccess)
      res.foreach { case (v, x) =>
        assert(Arrays.equals(v, value.getBytes("UTF-8")))
        assert(x.isEmpty)
      }
    }
    it("value not exists") {
      val res = client.getBytes("not_found")
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "404: DB: 7: no record: no record")
    }
  }

  describe("head") {
    it("value exists") {
      val key = asKey("test_key")
      val value = "test_value"
      prepare(key, value)
      val res = client.head(key)
      assert(res.isSuccess)
      res.foreach { case (l, x) =>
        assert(l === value.size)
        assert(x.isEmpty)
      }
    }
    it("value with xt exists") {
      val key = asKey("test_key_xt")
      val value = "test_value_xt"
      val xt = DateTime.now.plusMinutes(10)
      prepare(key, value, Some(xt.getMillis / 1000))
      val res = client.head(key)
      assert(res.isSuccess)
      res.foreach { case (l, x) =>
        assert(l === value.size)
        assert(x.exists(_.getMillis == xt.withMillisOfSecond(0).getMillis))
      }
    }
    it("value exists (key require url encode)") {
      val key = asKey("te st/key?=%~")
      val value = "te st/value?=%~"
      prepare(encode(key), value)
      val res = client.head(key)
      assert(res.isSuccess)
      res.foreach { case (l, x) =>
        assert(l === value.size)
        assert(x.isEmpty)
      }
    }
    it("value not exists") {
      val res = client.head("not_found")
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "404: DB: 7: no record: no record")
    }
  }

  describe("set") {
    it("set string value") {
      val key = asKey("test_key_for_set_string")
      val value = "test_value_for_set_string"
      assert(client.set(key, Value(value)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("set bytes value") {
      val key = asKey("test_key_for_set_bytes")
      val value = "test_value_for_set_bytes".getBytes("UTF-8")
      assert(client.set(key, Value(value)).isSuccess)

      val res = Http(restUrl(key)).asBytes
      assert(res.isNotError)
      assert(Arrays.equals(res.body, value))
      assert(getXt(res.headers).isEmpty)
    }
    it("set value with xt") {
      val key = asKey("test_key_for_set_with_xt")
      val value = "test_value_for_set_with_xt"
      val xt = DateTime.now.plusMinutes(10)
      assert(client.set(key, Value(value), Some(xt)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).contains(xt.withMillisOfSecond(0)))
    }
    it("set value (key require url encode)") {
      val key = asKey("te st/key_for_set?=%~")
      val value = "te st/value_for_set?=%~"
      assert(client.set(key, Value(value)).isSuccess)

      val res = Http(restUrl(encode(key))).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
  }

  describe("add") {
    it("add string value") {
      val key = asKey("test_key_for_add_string")
      val value = "test_value_for_add_string"
      assert(client.add(key, Value(value)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("add bytes value") {
      val key = asKey("test_key_for_add_bytes")
      val value = "test_value_for_add_bytes".getBytes("UTF-8")
      assert(client.add(key, Value(value)).isSuccess)

      val res = Http(restUrl(key)).asBytes
      assert(res.isNotError)
      assert(Arrays.equals(res.body, value))
      assert(getXt(res.headers).isEmpty)
    }
    it("add value with xt") {
      val key = asKey("test_key_for_add_with_xt")
      val value = "test_value_for_add_with_xt"
      val xt = DateTime.now.plusMinutes(10)
      assert(client.add(key, Value(value), Some(xt)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).contains(xt.withMillisOfSecond(0)))
    }
    it("add value (key require url encode)") {
      val key = asKey("te st/key_for_add?=%~")
      val value = "te st/value_for_add?=%~"
      assert(client.add(key, Value(value)).isSuccess)

      val res = Http(restUrl(encode(key))).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("add value (already key exists)") {
      val key = asKey("test_key_for_add_string")
      val value = "test_value_for_add_string"
      prepare(key, "prepared_value")

      val res = client.add(key, Value(value))
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "450: DB: 6: record duplication: record duplication")
    }
  }

  describe("replace") {
    it("replace string value") {
      val key = asKey("test_key_for_replace_string")
      val value = "test_value_for_replace_string"
      prepare(key, "prepared_value")
      assert(client.replace(key, Value(value)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("replace bytes value") {
      val key = asKey("test_key_for_replace_bytes")
      val value = "test_value_for_replace_bytes".getBytes("UTF-8")
      prepare(key, "prepared_value")
      assert(client.replace(key, Value(value)).isSuccess)

      val res = Http(restUrl(key)).asBytes
      assert(res.isNotError)
      assert(Arrays.equals(res.body, value))
      assert(getXt(res.headers).isEmpty)
    }
    it("replace value with xt") {
      val key = asKey("test_key_for_replace_with_xt")
      val value = "test_value_for_replace_with_xt"
      val xt = DateTime.now.plusMinutes(10)
      prepare(key, "prepared_value")
      assert(client.replace(key, Value(value), Some(xt)).isSuccess)

      val res = Http(restUrl(key)).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).contains(xt.withMillisOfSecond(0)))
    }
    it("replace value (key require url encode)") {
      val key = asKey("te st/key_for_replace?=%~")
      val value = "te st/value_for_replace?=%~"
      prepare(encode(key), "prepared_value")
      assert(client.replace(key, Value(value)).isSuccess)

      val res = Http(restUrl(encode(key))).asString
      assert(res.isNotError)
      assert(res.body === value)
      assert(getXt(res.headers).isEmpty)
    }
    it("replace value (key not exists)") {
      val key = asKey("test_key_for_replace_string_not_exists")
      val value = "test_value_for_replace_string_not_exists"
      val res = client.replace(key, Value(value))
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "450: DB: 7: no record: no record")
    }
  }

  describe("delete") {
    it("delete exist value") {
      val key = asKey("test_key")
      val value = "test_value"
      prepare(key, value)
      val res1 = Http(restUrl(key)).asString
      assert(res1.isNotError)
      assert(res1.body === value)

      assert(client.delete(key).isSuccess)
      val res2 = Http(restUrl(key)).asString
      assert(res2.code === 404)
      assert(getError(res2.headers).contains("DB: 7: no record: no record"))
    }
    it("delete not exist value") {
      val key = asKey("test_key")
      val res = client.delete(key)
      assert(res.isFailure)
      assert(res.failed.get.getMessage === "404: DB: 7: no record: no record")
    }
  }
}
