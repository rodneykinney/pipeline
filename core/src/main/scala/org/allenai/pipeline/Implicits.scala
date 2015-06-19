package org.allenai.pipeline

import java.nio.file.Files

import org.allenai.pipeline.InputFileArg.FileArtifactProducer
import org.apache.commons.io.FileUtils
import spray.json.JsonFormat

import java.io.File
import java.net.URI

trait Implicits {
  import scala.language.implicitConversions
  import spray.json._

  implicit def asFileArtifact(f: File) = new FileArtifact(f)
  implicit def asStructuredArtifact(f: File): StructuredArtifact = f match {
    case f if f.exists && f.isDirectory => new DirectoryArtifact(f)
    case _ => new ZipFileArtifact(f)
  }
  implicit def asFlatArtifact(url: URI) =
    CreateCoreArtifacts.fromFileUrls.urlToArtifact[FlatArtifact].apply(url)
  implicit def asStructuredArtifact(url: URI) =
    CreateCoreArtifacts.fromFileUrls.urlToArtifact[StructuredArtifact].apply(url)

  implicit def asStringSerializable[T](jsonFormat: JsonFormat[T]): StringSerializable[T] =
    new StringSerializable[T] {
      override def fromString(s: String): T = jsonFormat.read(s.parseJson)

      override def toString(data: T): String = jsonFormat.write(data).compactPrint
    }

  implicit def convertToProcessArg(s: String): StringArg = StringArg(s)
  implicit def convertToInputFile(input: (FlatArtifact, String)) = {
    val (artifact, name) = input
    InputFileArg(name, artifact)
  }
  implicit def convertPersistedProducerToInputFile[T, A <: FlatArtifact](
    input: (PersistedProducer[T, A], String)
  ) = {
    val (p, name) = input
    InputFileArg(name, p.copy(create = () => {
      p.get
      new FileArtifactProducer(p.artifact).get
    }))
  }

}
