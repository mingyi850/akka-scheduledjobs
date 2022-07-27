package com.minglim.akkascheduledjobs.scheduledjobsframework.util

import java.security.MessageDigest

trait HashGeneratorService {
  def generateHash(base: String, randomizer: String = ""): String
}

case class MD5HashGeneratorService() extends HashGeneratorService {
  override def generateHash(base: String, randomizer: String = ""): String = {
    MessageDigest.getInstance("MD5").digest((base + randomizer).getBytes).toString

  }
}
