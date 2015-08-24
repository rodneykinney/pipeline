package org.allenai.pipeline

import java.nio.file.Files

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
  implicit def convertToInputFile(input: (File, String)) = {
    val (file, name) = input
    InputFileArg(name, ReadFromArtifact(UploadFile, new FileArtifact(file)))
  }
  implicit def convertArtifactToInputFile(input: (FlatArtifact, String)) = {
    val (artifact, name) = input
    new InputFileArg(name, ReadFromArtifact(UploadFile, artifact))
  }
  implicit def convertPersistedProducerToInputFile[T, A <: FlatArtifact](
    input: (PersistedProducer[T, A], String)
  ) = {
    val (p, name) = input
    InputFileArg(name, p.copy(create = () => {
      p.get
      new ReadFromArtifact(UploadFile, p.artifact).get
    }))
  }

}
