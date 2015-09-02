package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

import java.net.URI

class TestGitRemoteDirectory extends UnitSpec {
  val remoteDir = GitRemoteDirectory(new URI("git://github.com/allenai/pipeline/tree/master/examples/src/test/resources"))
  remoteDir.remoteAddress should equal(GitAddress("github.com/allenai/pipeline","master","examples/src/test/resources"))

}
