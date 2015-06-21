package org.allenai.pipeline

import org.allenai.common.Resource
import org.allenai.common.testkit.{ScratchDirectory, UnitSpec}

import scala.io.Source

import java.io.{File, FileWriter, PrintWriter}
import java.lang.Thread.UncaughtExceptionHandler

class TestRunProcess extends UnitSpec with ScratchDirectory {

  import org.allenai.pipeline.IoHelpers._

  "RunProcess" should "create output files" in {
    val outputFile = new File(scratchDir, "testTouchFile/output")
    val outputArtifact = new FileArtifact(outputFile)
    val p = RunProcess("touch", OutputFileArg("outputFile"))
    val pp = new ProducerWithPersistence(p.outputFiles("outputFile"), UploadFile, outputArtifact)
    pp.get
    outputFile should exist
  }

    it should "have a signature dependent on the version ID" in {
      val cat1 = RunProcess("touch", OutputFileArg("empty.txt"))
      val cat2 = RunProcess("touch", OutputFileArg("empty.txt")).withVersionId("2.0")
      val cat2prime = RunProcess("touch", OutputFileArg("empty.txt")).withVersionId("2.0")

      cat1.stepInfo.signature.id should not equal(cat2.stepInfo.signature.id)
      cat2.stepInfo.signature.id should equal(cat2prime.stepInfo.signature.id)
    }

  it should "capture stdout" in {
    val echo = RunProcess("echo", "hello", "world")
    val stdout = Source.fromInputStream(echo.stdout.get).getLines.mkString("\n")
    stdout should equal("hello world")
  }

  it should "capture stdout newlines" in {
    val s = "An old silent pond...\\nA frog jumps into the pond,\\nsplash! Silence again.\\n"
    val echo = RunProcess("printf", s)
    val stdoutLines = Source.fromInputStream(echo.stdout.get).getLines.toList
    stdoutLines.size should equal(3)

    val persistedStdout = newPipeline("testStdout").persist(echo.stdout, SaveStream)
    Source.fromInputStream(persistedStdout.get).getLines.toList.size should equal(3)

  }

  it should "capture stderr" in {
    val noSuchParameter = new RunProcess(List("touch", "-x", "foo")) {
      override def requireStatusCode = Set(1)
    }
    val stderr = Source.fromInputStream(noSuchParameter.stderr.get).mkString
    stderr.size should be > 0

    val persistedStderr = newPipeline("testStderr").persist(noSuchParameter.stderr, SaveStream)
    Source.fromInputStream(persistedStderr.get).mkString.size should be > 0
  }

  it should "throw an exception if command is not found" in {
    val noSuchCommand = RunProcess("eccho", "hello", "world")
    val defaultHandler = Thread.getDefaultUncaughtExceptionHandler
    // Suppress logging of exception by background thread
    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = ()
    })
    an[Exception] shouldBe thrownBy {
      noSuchCommand.get
    }
    // Restore exception handling
    Thread.setDefaultUncaughtExceptionHandler(defaultHandler)
  }
  it should "read input files" in {
    val dir = new File(scratchDir, "testCopy")
    dir.mkdirs()
    val inputFile = new File(dir, "input")
    val outputFile = new File(dir, "output")
    Resource.using(new PrintWriter(new FileWriter(inputFile))) {
      _.println("Some data")
    }
    val outputArtifact = new FileArtifact(outputFile)

    val copy = new ProducerWithPersistence(
      RunProcess(
        "cp",
        inputFile -> "sourceFile",
        OutputFileArg("targetFile")
      )
        .outputFiles("targetFile"), UploadFile, outputArtifact
    )
    copy.get
    outputFile should exist
    Source.fromFile(outputFile).mkString should equal("Some data\n")

    copy.stepInfo.dependencies.size should equal(1)
    Workflow.upstreamDependencies(copy).size should equal(2)
  }

  it should "pipe stdin to stdout" in {
    val echo = RunProcess("echo", "hello", "world")
    val wc = RunProcess("wc", "-c").withStdin(echo.stdout)
    val result = wc.stdout.get
    Source.fromInputStream(result).mkString.trim.toInt should equal(12)
  }

  it should "accept implicit conversions" in {
    val pipeline = newPipeline("TestImplicits")
    val echoOutput = pipeline.persist(RunProcess("echo", "hello", "world").stdout, SaveStream, "EchoCommandStdout")
    val wc = RunProcess("wc", "-c", echoOutput -> "inputFile")
    val wc2 = RunProcess("wc", "-c").withStdin(echoOutput)
    pipeline.persist(wc.stdout, SaveStream, "CharacterCountFile")
    pipeline.persist(wc2.stdout, SaveStream, "CharacterCountStdin")
    pipeline.run("TestImplicits")

    RunProcess("wc", "-c", echoOutput.artifact -> "inputFile")
  }

  def newPipeline(name: String) = Pipeline(new File(scratchDir, name))
}
