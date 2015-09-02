package org.allenai.pipeline

import org.apache.commons.io.FileUtils

import java.io.File
import java.net.URI
import java.nio.file.Files

/** Checks out a directory from Git at a specific commit
 */
case class GitRemoteDirectory(url: URI) extends Producer[File] with Ai2StepInfo {
  override def create = {
    val tmpDir = Files.createTempDirectory(url.toString.replace('/','$')).toFile
    sys.addShutdownHook(FileUtils.deleteDirectory(tmpDir))


    // git clone git://github.com/rodneykinney/pipeline
    // git archive --format zip  ebce4559993aae710722b1e94d6b5d55b4b0fd5d:src/test > j.zip
    ???
  }

  lazy val remoteAddress: GitAddress = {
    def invalidUrl = sys.error(s"Git url must be of the form git://<remote-repo>/tree/<commit>/<path>. Found $url")
    url.getScheme match {
      case "git" =>
        val path = url.getPath.dropWhile(_ == '/').split('/')
        require(path.contains("tree"), invalidUrl)
        val remote = (url.getHost +: path.takeWhile(_ != "tree")).mkString("/")
        val commitPlusDir = url.getPath.split('/').dropWhile(_ != "tree").drop(1)
          require(commitPlusDir.size > 1, invalidUrl)
          val commit = commitPlusDir.head
          val dir = commitPlusDir.tail.mkString("/")
          GitAddress(remote, commit, dir)
      case null => invalidUrl
      case _ => invalidUrl
    }

  }

}

case class GitAddress(remote: String, commit: String, path: String)


