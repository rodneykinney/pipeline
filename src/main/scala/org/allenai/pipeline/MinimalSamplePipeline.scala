package org.allenai.pipeline

import java.io.{FileWriter, PrintWriter, File}
import java.nio.file.Files

import org.allenai.common.Resource
import org.allenai.pipeline.ExternalProcess.{OutputFileToken, InputFileToken}
import org.allenai.pipeline.IoHelpers._
import org.allenai.pipeline._
import org.apache.commons.io.FileUtils
import spray.json.DefaultJsonProtocol._

import scala.io.Source
import scala.reflect.ClassTag

class MinimalSamplePipeline extends Pipeline {

  def outDir() : File = {
    val dirCurrent = System.getProperty("user.dir");
    new File(new File(dirCurrent), "minimalSamplePipeline-out")
  }

  // BEGIN scratchDir: copy paste from trait ScratchDirectory,
  // but without the UnitTest part.
  val scratchDir: File = {
    val dir = Files.createTempDirectory(this.getClass.getSimpleName).toFile
    sys.addShutdownHook(delete(dir))
    dir
  }

  def beforeAll: Unit = require(
    scratchDir.exists && scratchDir.isDirectory,
    s"Unable to create scratch directory $scratchDir"
  )

  def afterAll: Unit = delete(scratchDir)

  private def delete(f: File) {
    if (f.isDirectory()) {
      f.listFiles.foreach(delete)
    }
    f.delete()
  }
  // END scratchDir


  // from org.allenai.pipeline.Pipeline.saveToFileSystem
  override def tryCreateArtifact[A <: Artifact: ClassTag] =
    CreateFileArtifact.relativeToDirectory[A](outDir) orElse
      super.tryCreateArtifact[A]
  
  val inputDir = new File("src/test/resources/pipeline/MinimalSamplePipeline")

  val pipeline = Pipeline.saveToFileSystem(outDir)

  def run(isDryRun:Boolean) = {
    val docDir = new DirectoryArtifact(new File(inputDir, "xml"))

    // BEGIN copy-paste from: it should "read input files":
    val dir = new File(outDir, "testCopy")
    dir.mkdirs()
    val inputFile = new File(dir, "input")
    val outputFile = new File(dir, "output")
    Resource.using(new PrintWriter(new FileWriter(inputFile))) {
      _.println("Some data")
    }
    val inputArtifact = new FileArtifact(inputFile)
    val outputArtifact = new FileArtifact(outputFile)

    val copy = ExternalProcess("cp", InputFileToken("input"), OutputFileToken("output"))(
      inputs = Map("input" -> Read.fromArtifact(StreamIo, inputArtifact))
    )
      .outputs("output").persisted(StreamIo, outputArtifact)
    copy.get
    // END from: it should "read input files":

    if(isDryRun)
      pipeline.dryRun(new File(dir, "dry-run-output"),"MinimalSamplePipeline")
    else
      pipeline.runOnly("MinimalSamplePipeline", copy)
  }

}

object MinimalSamplePipeline {
  def main(args: Array[String]): Unit = {
    val isDryRun = args.contains("--dry-run")
    val that = new MinimalSamplePipeline()
    try {
      that.beforeAll
      that.run(isDryRun)
    }
    finally {
      that.afterAll
    }
  }
}
