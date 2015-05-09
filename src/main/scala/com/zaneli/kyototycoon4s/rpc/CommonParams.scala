package com.zaneli.kyototycoon4s.rpc

case class CommonParams(
    db: Option[String] = None,
    cur: Option[Int] = None,
    waitKey: Option[String] = None,
    waitTime: Option[Int] = None,
    signal: Option[Signal] = None) {

  def toParams: Seq[(String, Any)] = {
    (db.map(("DB", _)) ::
      cur.map(("CUR", _)) ::
      waitKey.map(("WAIT", _)) ::
      waitTime.map(("WAITTIME", _)) :: Nil).flatten ++
      signal.map(_.toParams).getOrElse(Nil)
  }
}

object CommonParams {
  lazy val empty: CommonParams = CommonParams()
}

case class Signal(value: String, broad: Boolean = false) {
  def toParams: Seq[(String, Any)] = {
    val signal = ("SIGNAL", value)
    if (broad) {
      Seq(signal, ("SIGNALBROAD", ""))
    } else {
      Seq(signal)
    }
  }
}
