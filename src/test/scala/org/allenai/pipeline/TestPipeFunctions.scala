package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

import scala.util.Random

class TestPipeFunctions extends UnitSpec {
  import PipeFunctions._

  def foo() = {
    val seed = 55
    def rng() = {
      val rand = new Random(seed)
      (0 to 10).map(i => rand.nextDouble())
    }
    val closure1 = () => 1
    val input: Producer[Iterable[Double]] = rng _

    def exp(d: Iterable[Double]) = d.map(math.exp)
    val pp = input.pipeTo(exp)

    input.stepInfo.parameters should equal(Map("seed" -> seed.toString))
  }
  foo()

}
