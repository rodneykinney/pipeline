package org.allenai.pipeline

import scala.reflect.ClassTag

import java.io.File
import java.net.URI

/** Creates an Artifact from a URL
  */
trait ArtifactFactory {
  /** @param url The location of the Artifact.  The scheme (protocol) is used to determine the
    *            specific implementation.
    * @tparam A The type of the Artifact to create.  May be an abstract or concrete type
    * @return The artifact
    */
  def createArtifact[A <: Artifact: ClassTag](url: URI): A
}

object ArtifactFactory {
  def apply(urlHandler: UrlToArtifact, fallbackUrlHandlers: UrlToArtifact*): ArtifactFactory =
    new ArtifactFactory {
      val urlHandlerChain =
        if (fallbackUrlHandlers.size == 0) {
          urlHandler
        } else {
          UrlToArtifact.chain(urlHandler, fallbackUrlHandlers.head, fallbackUrlHandlers.tail: _*)
        }

      def createArtifact[A <: Artifact: ClassTag](url: URI): A = {
        val fn = urlHandlerChain.urlToArtifact[A]
        val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
        require(fn.isDefinedAt(url), s"Cannot create $clazz from $url")
        fn(url)
      }
    }

  def resolveUrl(rootUrl: URI)(s: String): URI = {
    val parsed = new URI(s)
    parsed.getScheme match {
      case null =>
        val fullPath = s"${rootUrl.getPath.reverse.dropWhile(_ == '/').reverse}/${parsed.getPath.dropWhile(_ == '/')}"
        new URI(
          rootUrl.getScheme,
          rootUrl.getHost,
          fullPath,
          rootUrl.getFragment
        )
      case _ => parsed
    }
  }
}

/** Supports creation of a particular type of Artifact from a URL.
  * Allows chaining together of different implementations that recognize different input URLs
  * and support creation of different Artifact types
  */
trait UrlToArtifact {
  /** Return a PartialFunction indicating whether the given Artifact type can be created from an input URL
    * @tparam A The Artifact type to be created
    * @return A PartialFunction where isDefined will return true if an Artifact of type A can
    *         be created from the given URL
    */
  def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A]
}

object UrlToArtifact {
  // Chain together a series of UrlToArtifact instances
  // The result will be a UrlToArtifact that supports creation of the union of Artifact types and input URLs
  // that are supported by the individual inputs
  def chain(first: UrlToArtifact, second: UrlToArtifact, others: UrlToArtifact*) =
    new UrlToArtifact {
      override def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
        var fn = first.urlToArtifact[A] orElse second.urlToArtifact[A]
        for (o <- others) {
          fn = fn orElse o.urlToArtifact[A]
        }
        fn
      }
    }

  object Empty extends UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] =
      PartialFunction.empty[URI, A]
  }

}

object CreateCoreArtifacts {
  // Create a FlatArtifact or StructuredArtifact from an absolute file:// URL
  val fromFileUrls: UrlToArtifact = new UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && "file" == url.getScheme =>
          new FileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && null == url.getScheme =>
          new FileArtifact(new File(url.getPath)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[DirectoryArtifact])
          && "file" == url.getScheme
          && new File(url).exists
          && new File(url).isDirectory =>
          new DirectoryArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[DirectoryArtifact])
          && null == url.getScheme
          && new File(url.getPath).exists
          && new File(url.getPath).isDirectory =>
          new DirectoryArtifact(new File(url.getPath)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && "file" == url.getScheme =>
          new ZipFileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == url.getScheme =>
          new ZipFileArtifact(new File(url.getPath)).asInstanceOf[A]
      }
      fn
    }
  }
}
