package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.UnitSpec

import scala.util.Random

class TestPipeFunctions extends UnitSpec {

  import PipeFunctions._
  import TestFunctionsInObject._

  def newPipeline = Pipeline(new File("pipeline-output"))

  "PipeFunctions" should "infer function name" in {
    val rng = (randomNumbers _).withNoInputs
    checkInfo(rng, "randomNumbers")()()

    val rng2 = {
      val seed = 55
      (() => randomNumbersWithSeed(seed))
    }.withNoInputs.build
    checkInfo(rng2, "randomNumbersWithSeed")()()

    val exp = (takeExp _).withInput("input" -> rng)
    checkInfo(exp, "takeExp")()("input" -> rng)

    val log = (takeLog _).withInput("input" -> exp)
    checkInfo(log, "takeLog")()("input" -> exp)

    val tl: (Iterable[Double]) => Iterable[Double] = takeLog
    checkInfo(tl.withInput("input" -> exp), "takeLog")()("input" -> exp)

    val multiplied = (multiplyLists _).withInputs("first" -> exp, "second" -> log)
    checkInfo(multiplied, "multiplyLists")()("first" -> exp, "second" -> log)

    def localRandomNumbers(): Iterable[Double] = {
      val rand = new Random(55)
      (0 to 10).map(i => rand.nextDouble())
    }
    checkInfo((localRandomNumbers _).withNoInputs.build, "localRandomNumbers")()()

    val localRandomNumbersVal = () => {
      val rand = new Random(55)
      (0 to 10).map(i => rand.nextDouble())
    }
    checkInfo((localRandomNumbersVal).withNoInputs.build, "localRandomNumbersVal")()()

  }

  it should "persist producers" in {
    import IoHelpers._
    import spray.json.DefaultJsonProtocol._
    implicit val pipeline = newPipeline
    val rng = (randomNumbers _).withNoInputs.persisted(LineCollectionIo.text[Double])
    val rng2 = {
      val seed = 55
      (() => randomNumbersWithSeed(seed))
    }.withNoInputs.persisted(LineCollectionIo.json[Double])
    val exp = (takeExp _).withInput("input" -> rng).persisted(LineCollectionIo.text[Double])
  }

  it should "infer parameters" in {
    val rng2 = {
      val seed = 55
      (() => randomNumbersWithSeed(seed))
    }.withNoInputs.build
    //    checkInfo(rng2, "randomNumbersWithSeed")("seed" -> "55")()
  }

  it should "fail on non-deterministic functions" in {
    //    an[Exception] should be thrownBy {
    val rng = (randomNumbers _).withNoInputs
    val addRand = (addRandom _).withInput("input" -> rng)
    //    }
  }

  def checkInfo[T](p: Producer[T], name: String)(
    params: (String, String)*
  )(
    deps: (String, Producer[_])*
  ) = {
    p.stepInfo.className should equal(name)
    p.stepInfo.parameters.toSet should equal(params.toSet)
    p.stepInfo.dependencies.toSet should equal(deps.toSet)
  }

  def runAndOpen(pipeline: Pipeline): Unit = {
    pipeline.run("Test Run")
    pipeline.openDiagram()
  }
}

object TestFunctionsInObject {
  def randomNumbers() = randomNumbersWithSeed(55)
  def randomNumbersWithSeed(seed: Int): Iterable[Double] = {
    val rand = new Random(seed)
    (0 to 10).map(i => rand.nextDouble())
  }
  def takeExp(d: Iterable[Double]) = d.map(math.exp)
  def takeLog(d: Iterable[Double]) = d.map(math.log)
  def multiplyLists(d1: Iterable[Double], d2: Iterable[Double]) =
    d1.zip(d2).map { case (x, y) => x * y }
  def addRandom(d: Iterable[Double]) = d.zip(randomNumbers()).map { case (a, b) => a + b }
}
