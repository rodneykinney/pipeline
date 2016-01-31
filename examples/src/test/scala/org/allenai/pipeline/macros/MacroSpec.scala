package org.allenai.pipeline.macros

import org.allenai.common.testkit.UnitSpec

class MacroSpec extends UnitSpec {
  import org.allenai.pipeline.PipelineMacros._
  import scala.language.postfixOps

  "Macros" should "work" in {
    val f: () => Int = () => 117
    val fm = memoize(f)
//    val fm = f memoized
    fm()
  }

}
