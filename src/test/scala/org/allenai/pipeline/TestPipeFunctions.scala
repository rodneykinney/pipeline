package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.UnitSpec

import scala.util.Random

class TestPipeFunctions extends UnitSpec {

  import PipeFunctions._
  import TestFunctionsInObject._

  def newPipeline = Pipeline(new File("pipeline-output"))

  "PipeFunctions" should "handle basic functions" in {
    val pipeline = Pipeline(new File("pipeline-output"))
    val rng = (randomNumbers _).withNoInputs
    val exp = (takeExp _).withInput("input" -> rng)
    val log = (takeLog _).withInput("input" -> exp)
    val multiplied = (multiplyLists _).withInputs("first" -> exp, "second" -> log)

    def checkInfo[T](p: Producer[T], name: String)(
      params: (String, String)*
    )(
      deps: (String, Producer[_])*
    ) = {
      p.stepInfo.className should equal(name)
      p.stepInfo.parameters.toSet should equal(params.toSet)
      p.stepInfo.dependencies.toSet should equal(deps.toSet)
    }

    checkInfo(rng, "randomNumbers")()()
    checkInfo(exp, "takeExp")()("input" -> rng)
    checkInfo(log, "takeLog")()("input" -> exp)
    checkInfo(multiplied, "multiplyLists")()("first" -> exp, "second" -> log)

    an[Exception] should be thrownBy {
      val addRand = (addRandom _).withInput("input" -> multiplied)
    }
  }

  def runAndOpen(pipeline: Pipeline): Unit = {
    pipeline.run("Test Run")
    pipeline.openDiagram()
  }
}

object TestFunctionsInObject {
  def randomNumbers() = {
    val rand = new Random(55)
    (0 to 10).map(i => rand.nextDouble())
  }
  def takeExp(d: Iterable[Double]) = d.map(math.exp)
  def takeLog(d: Iterable[Double]) = d.map(math.log)
  def multiplyLists(d1: Iterable[Double], d2: Iterable[Double]) =
    d1.zip(d2).map { case (x, y) => x * y }
  def addRandom(d: Iterable[Double]) = d.zip(randomNumbers()).map { case (a, b) => a + b }
}
