package com.zaneli.kyototycoon4s

import com.github.nscala_time.time.Imports.DateTime
import java.net.URLEncoder
import java.util.Arrays
import org.scalatest.FunSpec
import scalaj.http.Http

class RestClientSpec extends FunSpec {

  private[this] val (host, port) = ("localhost", 1978)

  private[this] val client = KyotoTycoonClient.rest(host, port)

  describe("getString") {
    it("value exists") {
      val key = "test_key"
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
      val key = "test_key_xt"
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
      val key = "te st/key?=%~"
      val value = "te st/value?=%~"
      prepare(URLEncoder.encode(key, "UTF-8"), value)
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
      val key = "test_key"
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
      val key = "test_key_xt"
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
      val key = "te st/key?=%~"
      val value = "te st/value?=%~"
      prepare(URLEncoder.encode(key, "UTF-8"), value)
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
      val key = "test_key"
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
      val key = "test_key_xt"
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
      val key = "te st/key?=%~"
      val value = "te st/value?=%~"
      prepare(URLEncoder.encode(key, "UTF-8"), value)
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

  private[this] def prepare(key: String, value: String, xt: Option[Long] = None): Unit = {
    val req = Http(s"http://$host:$port/$key").postData(value).method("put")
    xt.fold(req)(x => req.header("X-Kt-Xt", x.toString)).asString
  }
}
