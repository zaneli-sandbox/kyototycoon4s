# kyototycoon4s
[KyotoTycoon](http://fallabs.com/kyototycoon/) client library for Scala.

[![Build Status](https://api.travis-ci.org/zaneli/kyototycoon4s.png?branch=master)](https://travis-ci.org/zaneli/kyototycoon4s)

## Requirements

* JDK 8
* Scala 2.11.x

## Usage

### RESTful-style

```scala
> import com.zaneli.kyototycoon4s.KyotoTycoonClient
> import com.zaneli.kyototycoon4s.Implicits._
> import com.github.nscala_time.time.Imports.DateTime

> val client = KyotoTycoonClient.rest("localhost", 1978)

> client.get("key_without_xt") // get value as string
Success(Record(value, None))

> client.get("key_with_xt")
Success(Record(value, Some(2015-05-05T16:40:21.000+09:00)))

> client.get("key_binary", as = identity) // get value as byte array
Success(Record([B@1e22292d, None))

> client.head("key_without_xt")
Success((5, None)) // return Content-Length.

> client.head("key_with_xt")
Success((5, Some(2015-05-05T16:40:21.000+09:00))) // return Content-Length and expiration time.

> client.set("key_without_xt", "value")
Success(())

> client.set("key_with_xt", "value", Some(DateTime.now.plusMinutes(10)))
Success(())

> client.set("key_num", 100L)
Success(())

> client.add("other_key", "value")
Success(())

> client.replace("key", "replaced_value")
Success(())

> client.delete("key")
Success(())
```
