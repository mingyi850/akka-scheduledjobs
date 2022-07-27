package com.minglim.akkascheduledjobs.scheduledjobsframework.model

object DC extends Enumeration {
  type DC = Value
  val HKG    = Value("hkg")
  val SGP    = Value("sgp")
  val ASH    = Value("ash")
  val AMS    = Value("ams")
  val SHA    = Value("sha")
  val BKK    = Value("bkk")
  val LOCAL  = Value("local")
  val DOCKER = Value("docker")

  def parse(str: String): DC = {
    str match {
      case "as" | "ash" => DC.ASH
      case "am" | "ams" => DC.AMS
      case "sg" | "sgp" => DC.SGP
      case "hk" | "hkg" => DC.HKG
      case "docker"     => DC.DOCKER
      case "local"      => DC.LOCAL
    }
  }
}
