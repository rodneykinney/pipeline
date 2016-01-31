package org.allenai.pipeline.macros

import org.allenai.pipeline.PipelineMacros
import org.allenai.pipeline._

/**
 * Created by rodneykinney on 1/1/16.
 */
object TryMacros {
  import PipelineMacros._
  import Pipes._
  import scala.language.postfixOps

  def main(args: Array[String]) {
    val f: () => Int = () => 1
    val fm = memoize(f)
    fm()

    val fm2 = memoize(FunctionHolder.newNumber)
    fm2()

    def f3(): Int = 3
    val fm3 = memoize(f3)
    fm3()

    val f4 = FunctionHolder.curried(4) _
    val fm4 = memoize(f4)
    fm4()

    def add3(x: Int) = x + 3
    val add3Val: Int => Int = x => add3(x)
    val fmAdd3 = f4 | (add3 _)

    def addTogether(x: Int, y: Int) = x+y

    val pipeline = buildPipeline (new PFunc0(f) | add3Val)
    val result = pipeline.get
    println(result)
  }
}

object FunctionHolder {
  def newNumber(): Int = 2

  def curried(x: Int)(): Int = x
}

object Pipes {
  import PipelineMacros._
  implicit class Memoizable[T](f: () => T) {
    def memoized = memoize(f)
  }

  implicit class Pipeable[T](f: () => T) {
    def |[S](fn: T => S): () => S = () => fn(f())
  }

}

