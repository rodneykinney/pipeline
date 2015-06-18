package org.allenai.pipeline

import org.apache.commons.io.FileUtils

import scala.io.Source

import java.io.{ByteArrayInputStream, File, FileWriter, InputStream}
import java.nio.file.Files

/** Executes an arbitrary system process
  * @param commandTokens   The set of tokens that comprise the command to be executed.
  *                        Each token is either:
  *                        a String
  *                        a Placeholder representing an input data file
  *                        a Placeholder representing an output data file
  *                        Examples:
  *                        StringToken("cp") InputFileToken("src") OutputFileToken("target")
  *                        StringToken("python") InputFileToken("script") StringToken("-o") OutputFileToken("output")
  *
  *                        Producers based on ExternalProcess should be created with class RunExternalProcess.
  */

case class RunProcess(args: ProcessArg*) extends Producer[ProcessOutput] with Ai2SimpleStepInfo {
  {
    val outputFileNames = args.collect { case arg: OutputFileArg => arg}.map(_.name)
    require(outputFileNames.distinct.size == outputFileNames.size, {
      val duplicates = outputFileNames.groupBy(x => x).filter(_._2.size > 1).map(_._1)
      s"Duplicate output names: ${duplicates.mkString("[", ",", "]")}"
    })
  }

  override def create = {
    val scratchDir = Files.createTempDirectory(null).toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(scratchDir))

    import scala.sys.process._
    val captureStdoutFile = new File(scratchDir, "stdout")
    val captureStderrFile = new File(scratchDir, "stderr")
    val out = new FileWriter(captureStdoutFile)
    val err = new FileWriter(captureStderrFile)

    val logger = ProcessLogger(
      (o: String) => out.append(o),
      (e: String) => err.append(e)
    )

    val command = cmd(scratchDir)

    val stdInput = args.collect { case arg: StdInput => arg.inputStream.get()}.headOption.getOrElse(new ByteArrayInputStream(Array.emptyByteArray))

    val status = (command #< stdInput) ! logger
    out.close()
    err.close()

    require(requireStatusCode.contains(status),
      s"Command $command failed with status $status: ${Source.fromFile(captureStderrFile).getLines.take(100).mkString("\n")}")

    val outputFiles = args.collect {
      case arg: OutputFileArg => (arg.name, new FileArtifact(new File(scratchDir, arg.name)))
    }.toMap


    ProcessOutput(status,
      new FileArtifact(captureStdoutFile),
      new FileArtifact(captureStderrFile),
      outputFiles)
  }

  // Fail if the external process returns with a status code not included in this set
  def requireStatusCode = Set(0)

  val outer = this

  def stdout: Producer[FileArtifact] =
    this.copy(
      create = () => outer.get.stdout,
      stepInfo = () => PipelineStepInfo("stdout").addParameters("cmd" -> outer))

  def stderr: Producer[FileArtifact] =
    this.copy(
      create = () => outer.get.stderr,
      stepInfo = () => PipelineStepInfo("stderr").addParameters("cmd" -> outer))

  def outputFiles: Map[String, Producer[FileArtifact]] =
    args.collect {
      case OutputFileArg(name) =>
        (name,
          this.copy(
            create = () => outer.get.outputs(name),
            stepInfo = () => PipelineStepInfo(name).addParameters("cmd" -> outer)))
    }.toMap

  def cmd(scratchDir: File): Seq[String] = {
    args.map {
      case InputFileArg(_, p) => p.get.file.getCanonicalPath
      case output: OutputFileArg => new File(scratchDir, output.name).getCanonicalPath
      case arg => arg.name
    }
  }

  override def stepInfo = {
    val inputFiles = for (InputFileArg(name, p) <- args) yield (name, p)
    val stdInput = for (StdInput(p) <- args) yield ("stdin", p)
    val cmd = args.collect {
      case InputFileArg(name, _) => s"<$name>"
      case OutputFileArg(name) => s"<$name>"
      case StringArg(name) => name
    }
    super.stepInfo
      .addParameters("cmd" -> cmd.mkString(" "))
      .addParameters(inputFiles: _*)
      .addParameters(stdInput: _*)
  }

}

trait ProcessArg {
  def name: String
}

case class InputFileArg(name: String, inputFile: Producer[FileArtifact]) extends ProcessArg

case class OutputFileArg(name: String) extends ProcessArg

case class StdInput(inputStream: Producer[() => InputStream]) extends ProcessArg {
  def name = "stdin"
}

case class StringArg(name: String) extends ProcessArg

case class ProcessOutput(
  returnCode: Int,
  stdout: FileArtifact,
  stderr: FileArtifact,
  outputs: Map[String, FileArtifact]
  )
