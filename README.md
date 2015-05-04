# kyototycoon4s
[KyotoTycoon](http://fallabs.com/kyototycoon/) client library for Scala.

[![Build Status](https://api.travis-ci.org/zaneli/kyototycoon4s.png?branch=master)](https://travis-ci.org/zaneli/kyototycoon4s)

## Requirements

* JDK 8
* Scala 2.11.x

## Usage

### RESTful-style

```scala
> import com.zaneli.kyototycoon4s.{KyotoTycoonClient, Value}
> import com.github.nscala_time.time.Imports.DateTime

> val client = KyotoTycoonClient.rest("localhost", 1978)

> client.getString("key_without_xt")
Success((value, None))

> client.getString("key_with_xt")
Success((value, Some(2015-05-05T16:40:21.000+09:00)))

> client.getBytes("key_without_xt")
Success(([B@1e22292d, None))

> client.head("key_without_xt")
Success((5, None)) // return Content-Length.

> client.head("key_with_xt")
Success((5, Some(2015-05-05T16:40:21.000+09:00))) // return Content-Length and expiration time.

> client.set("key_without_xt", Value("value")) // write value as string.
Success(())

> client.set("key_with_xt", Value("value"), Some(DateTime.now.plusMinutes(10)))
Success(())

> client.set("key", Value("value".getBytes("UTF-8"))) // write value as byte array.
Success(())

> client.add("other_key", Value("value"))
Success(())

> client.replace("key", Value("replaced_value"))
Success(())

> client.delete("key")
Success(())
```
