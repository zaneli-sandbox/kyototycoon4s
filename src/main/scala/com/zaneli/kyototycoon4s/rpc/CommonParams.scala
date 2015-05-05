package com.zaneli.kyototycoon4s.rpc

case class CommonParams(
    db: Option[String] = None, cur: Option[String] = None, waitKey: Option[String] = None, waitTime: Option[Int] = None) {

  def toParams: Seq[(String, Any)] = {
    val params = db.map(("DB", _)) :: cur.map(("CUR", _)) :: waitKey.map(("WAIT", _)) :: waitTime.map(("WAITTIME", _)) :: Nil
    params.flatten
  }
}

object CommonParams {
  lazy val empty: CommonParams = CommonParams()
}
