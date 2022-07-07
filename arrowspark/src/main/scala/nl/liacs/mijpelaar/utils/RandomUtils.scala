package nl.liacs.mijpelaar.utils

import scala.util.Random

class RandomUtils(val rand: Random) {
  def generateRandomNumber(start: Int = 0, end: Int = Integer.MAX_VALUE-1) : Int = {
    start + rand.nextInt( (end-start) + 1)
  }
}
