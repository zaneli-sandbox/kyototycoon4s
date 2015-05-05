package com.zaneli.kyototycoon4s.rpc

sealed abstract class Origin[+A] {
  def value: String
}

object Origin {
  def num[A](num: A): Num[A] = {
    Num(num)
  }

  case object Try extends Origin[Nothing] {
    override val value = "try"
  }
  case object Set extends Origin[Nothing] {
    override val value = "set"
  }
  case class Num[A] private[Origin] (num: A) extends Origin[A] {
    override val value = num.toString
  }
}
