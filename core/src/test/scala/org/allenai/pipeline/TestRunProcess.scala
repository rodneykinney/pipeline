package org.allenai.pipeline

import org.allenai.common.testkit.{ScratchDirectory, UnitSpec}

import scala.io.Source

import java.io.File

/** Created by rodneykinney on 5/14/15.
  */
class TestRunProcess extends UnitSpec with ScratchDirectory {

  import org.allenai.pipeline.IoHelpers._

  "RunProcess" should "create output files" in {
    val outputFile = new File(scratchDir, "testTouchFile/output")
    val outputArtifact = new FileArtifact(outputFile)
    val p = RunProcess("touch", OutputFileArg("outputFile"))
    val pp = new ProducerWithPersistence(p.outputFiles("outputFile"), CopyFlatArtifact, outputArtifact)
    pp.get
    outputFile should exist
  }

  //
  //  def CatFileForTest(scratchDir: File, rfile: String, vh1: Seq[String]): Producer[() => InputStream] = {
  //    val fa = new FileArtifact(new File(scratchDir, rfile))
  //    RunExternalProcess(ScriptToken("cat"), InputFileToken("target"))(
  //      Seq(fa),
  //      versionHistory = vh1
  //    ).stdout
  //  }
  //  "ScriptToken" should "be invariant of home directory" in {
  //    val proc1 = ExtprocForScriptTokenTest(scratchDir, "/home/bert/src/pipelineFoo/supersort.pl")
  //    val proc2 = ExtprocForScriptTokenTest(scratchDir, "/home/ernie/src/pipelineFoo/supersort.pl")
  //    proc1.stepInfo.signature.id should equal(proc2.stepInfo.signature.id)
  //  }
  //
  //  def ExtprocForScriptTokenTest(scratchDir: File, afile: String) = {
  //    val f = new File(scratchDir, afile)
  //    RunExternalProcess(ScriptToken(f.getAbsolutePath()))(Seq()).stdout
  //  }
  //
  //  "RunExternalProcess signature" should "be invariant between identical producers" in {
  //    val rfile = "in1.txt"
  //    Resource.using(new java.io.PrintWriter(new java.io.FileWriter(new File(scratchDir, rfile)))) {
  //      _.println("hello")
  //    }
  //    val pipeline = Pipeline(new File(scratchDir, "ExtargSignature"))
  //    val cat1 = CatFileForTest(scratchDir, rfile, List("v1.0"))
  //    val cat2 = CatFileForTest(scratchDir, rfile, List("v1.0"))
  //    val sig1 = cat1.stepInfo.signature.infoString
  //    val sig2 = cat2.stepInfo.signature.infoString
  //    sig1 should equal(sig2)
  //  }
  //
    it should "capture stdout" in {
      val echo =  RunProcess("echo", "hello", "world")
      val stdout = Source.fromInputStream(echo.stdout.get.read).getLines.mkString("\n")
      stdout should equal("hello world")
    }

    it should "capture stdout newlines" in {
      val s = "An old silent pond...\\nA frog jumps into the pond,\\nsplash! Silence again.\\n"
      val echo = RunProcess("printf", s)
      val stdoutLines = Source.fromInputStream(echo.stdout.get.read).getLines.toList
      stdoutLines.size should equal(3)
    }
  //
  //  it should "capture stderr" in {
  //    val noSuchParameter = new ExternalProcess("touch", "-x", "foo")
  //    val stderr = IOUtils.readLines(noSuchParameter.run(Seq()).stderr()).asScala.mkString("\n")
  //    stderr.size should be > 0
  //  }
  //  it should "throw an exception if command is not found" in {
  //    val noSuchCommand = new ExternalProcess("eccho", "hello", "world")
  //    val defaultHandler = Thread.getDefaultUncaughtExceptionHandler
  //    // Suppress logging of exception by background thread
  //    Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler {
  //      override def uncaughtException(t: Thread, e: Throwable): Unit = ()
  //    })
  //    an[Exception] shouldBe thrownBy {
  //      noSuchCommand.run(Seq())
  //    }
  //    // Restore exception handling
  //    Thread.setDefaultUncaughtExceptionHandler(defaultHandler)
  //  }
  //  it should "read input files" in {
  //    val dir = new File(scratchDir, "testCopy")
  //    dir.mkdirs()
  //    val inputFile = new File(dir, "input")
  //    val outputFile = new File(dir, "output")
  //    Resource.using(new PrintWriter(new FileWriter(inputFile))) {
  //      _.println("Some data")
  //    }
  //    val inputArtifact = new FileArtifact(inputFile)
  //    val outputArtifact = new FileArtifact(outputFile)
  //
  //    val copy = new ProducerWithPersistence(RunExternalProcess(ScriptToken("cp"), InputFileToken("input"), OutputFileToken("output"))(
  //      inputs = Seq(inputArtifact)
  //    )
  //      .outputs("output"), StreamIo, outputArtifact)
  //    copy.get
  //    outputFile should exist
  //    Source.fromFile(outputFile).mkString should equal("Some data\n")
  //
  //    copy.stepInfo.dependencies.size should equal(1)
  //    Workflow.upstreamDependencies(copy).size should equal(2)
  //  }
  //
  //  it should "pipe stdin to stdout" in {
  //    val echo = new ExternalProcess("echo", "hello", "world")
  //    val wc = new ExternalProcess("wc", "-c")
  //    val result = wc.run(Seq(), stdinput = echo.run(Seq()).stdout)
  //    IOUtils.readLines(result.stdout()).asScala.head.trim().toInt should equal(11)
  //  }
  //
  //  def consumeExtargForCoerceTest(absdScript: String, a: Extarg) = {
  //    RunExternalProcess(ScriptToken("cat"), InputFileToken(""))(
  //      Seq(a),
  //      versionHistory = Seq("v1.0")
  //    )
  //  }
  //
  //  it should "coerce a Producer[() -> InputStream] to Extarg" in {
  //    import org.allenai.pipeline.ExternalProcess._
  //    val echo = RunExternalProcess("echo", "hello", "world")(Seq())
  //    val cat1 = consumeExtargForCoerceTest("", echo.stdout) // should find convertProducerToInputData
  //  }
  //
  //  it should "coerce a PersistedProducer[() -> InputStream,_] to Extarg" in {
  //    import org.allenai.pipeline.ExternalProcess._
  //    val pipeline = Pipeline(scratchDir)
  //    val echo = RunExternalProcess("echo", "hello", "world")(Seq())
  //    val echop = pipeline.persist(echo.stdout, StreamIo, "hint_out")
  //    val cat1 = consumeExtargForCoerceTest("", echop) // finds convertPersistedProducer1ToInputData
  //    // TODO: why isn't convertPersistedProducer2ToInputData enough?
  //  }
  //
  //  it should "coerce an artifact to Extarg" in {
  //    import org.allenai.pipeline.ExternalProcess._
  //
  //    val dir = new File(scratchDir, "testCoerce")
  //    dir.mkdirs()
  //    val inputFile = new File(dir, "a1")
  //    Resource.using(new PrintWriter(new FileWriter(inputFile))) {
  //      _.println("Some data")
  //    }
  //    val inputArtifact = new FileArtifact(inputFile)
  //
  //    val cat1 = consumeExtargForCoerceTest("", inputArtifact) // should find convertArtifactToInputData
  //  }
}
