package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.UnitSpec

import scala.util.Random

class TestPipeFunctions extends UnitSpec {
  import IoHelpers._
  import PipeFunctions._

  val seed = 55
  val rng = () => {
    val rand = new Random(seed)
    (0 to 10).map(i => rand.nextDouble())
  }

  "PipeFunctions" should "convert no-arg functions" in {
    val pipeline = Pipeline(new File("pipeline-output"))
    pipeline.Persist.Collection.asText[Double](rng.withName("Initial list"))
  }

  it should "convert single-arg functions" in {
    val pipeline = Pipeline(new File("pipeline-output"))
    def expOfList(d: Iterable[Double]) = d.map(math.exp)
    val exp = rng.pipeTo(expOfList)

    def logOfList(d: Iterable[Double]) = d.map(math.log)
    val log = exp.pipeTo(logOfList)

    pipeline.Persist.Collection.asText[Double](log)
  }
}
