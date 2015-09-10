package org.allenai.pipeline

import java.io.File

import org.allenai.common.testkit.UnitSpec

import scala.util.Random

class TestPipeFunctions extends UnitSpec {

  import IoHelpers._
  import PipeFunctions._
  import TestFunctionsInObject._

  def newPipeline = Pipeline(new File("pipeline-output"))

  "PipeFunctions" should "convert no-arg functions" in {
    val pipeline = Pipeline(new File("pipeline-output"))
    pipeline.Persist.Collection.asText[Double](rng.withNoInputs)
  }

  it should "convert single-arg functions" in {
    val pipeline = newPipeline
    val exp = (expOfList _).withInput(rng.withNoInputs)

    val log = (logOfList _).withInput(exp)

    pipeline.Persist.Collection.asText[Double](log)
  }

  it should "convert two-arg functions" in {
    val pipeline = newPipeline

    val exp = (expOfList _).withInput(rng.withNoInputs)

    val log = (logOfList _).withInput(exp)

    val multiplied = (multiplyLists _).withInputs(exp, log)
    pipeline.Persist.Collection.asText[Double](multiplied)
  }

  it should "persist producers" in {
    implicit val pipeline = newPipeline

    val exp = (expOfList _)
      .withInput(rng.withNoInputs)
      .persisted(LineCollectionIo.text[Double])

    pipeline.persistedSteps.values should contain(exp)
  }

  def runAndOpen(pipeline: Pipeline): Unit = {
    pipeline.run("Test Run")
    pipeline.openDiagram()
  }
}

object TestFunctionsInObject {
  val seed = 55
  val rng = () => {
    val rand = new Random(seed)
    (0 to 10).map(i => rand.nextDouble())
  }
  def expOfList(d: Iterable[Double]) = d.map(math.exp)
  def logOfList(d: Iterable[Double]) = d.map(math.log)
  def multiplyLists(d1: Iterable[Double], d2: Iterable[Double]) =
    d1.zip(d2).map { case (x, y) => x * y }
}
