package org.allenai.pipeline

import scala.reflect.ClassTag

object PipeFunctions {

  import scala.language.implicitConversions

  implicit def Build[T](builder: ProducerBuilder[T]): Producer[T] = builder.build

  abstract class ProducerBuilder[T](
      val impl: AnyRef,
      val inputs: (String, Producer[_])*
  ) {
    outer =>
    val usage = new ClosureAnalyzer(impl)
    protected[this] var info: PipelineStepInfo = PipelineStepInfo("")

    def withName(name: String) = {
      info = info.copy(className = name)
      this
    }
    def addParameters(args: (String, Any)*) = {
      info = info.addParameters(args: _*)
      this
    }
    def withParameters(args: (String, Any)*) = {
      info = info.copy(parameters = args.map { case (k, v) => (k, String.valueOf(v)) }.toMap)
      this
    }
    def persisted[A <: Artifact: ClassTag](
      io: Serializer[T, A] with Deserializer[T, A]
    )(
      implicit
      pipeline: Pipeline
    ) =
      pipeline.persist(build, io)
    def create(): T
    lazy val build = {
      val name = usage.firstExteriorMethod.map(_.name).getOrElse(impl.getClass.getName)
      val localInfo = info.copy(
        className = name,
        dependencies = inputs.toMap
      )
      new Producer[T] {
        override def create = outer.create()
        override def stepInfo = localInfo
      }
    }
  }

  implicit class ProducerBuilder0[T](func: () => T) {
    def withNoInputs = new ProducerBuilder[T](func) {
      def create() = func()
    }
  }

  implicit class ProducerBuilder1[I, O](func: I => O) {
    def withInput(input: (String, Producer[I])) = {
      new ProducerBuilder[O](func, input) {
        override def create() = func(input._2.get)
      }
    }
  }
  implicit class ProducerBuilder2[I1, I2, O](func: (I1, I2) => O) {
    def withInputs(
      input1: (String, Producer[I1]),
      input2: (String, Producer[I2])
    ) = {
      new ProducerBuilder[O](func, input1, input2) {
        override def create() = func(input1._2.get, input2._2.get)
      }
    }
  }
}

class PFunc0[O](fn: () => O) extends Function0[O] {
  private lazy val cached = fn()
  def apply = cached

  def |[S](fn1: O => S): PFunc0[S] = new PFunc1(fn1, this)
}

class PFunc1[I,O](fn1: I => O, inputs: PFunc0[I]) extends PFunc0[O](() => fn1(inputs()))

object PipelineMacros {

  import language.experimental.macros
  import reflect.macros.blackbox.Context
//  implicit class MM[T](f: () => T) {
//    def mem: () => T = mem_impl(f)
//    private def mem_impl(f: () => T) = macro memoize_impl[T]
//  }

  def memoize[T](f: () => T): () => T = macro memoize_impl[T]

  def buildPipeline[T](f: PFunc0[T]): Producer[T] = macro pipeline_impl[T]

  def pipeline_impl[T: c.WeakTypeTag](c: Context)(f: c.Expr[PFunc0[T]]): c.Expr[Producer[T]] =  {
    import c.universe._
    println(s"pipeline_impl: ${f.tree}")
    val name = c.Expr[String](Literal(Constant(show(f.tree))))
    val r = reify { new Producer[T] {
      def stepInfo = PipelineStepInfo(name.splice)
      def create = f.splice.apply()
    }}
    r
//    reify { null: Producer[T]}
  }

  def memoize_impl[T : c.WeakTypeTag](c: Context)(f: c.Expr[() => T]): c.Expr[() => T] =  {
    import c.universe._
    import scala.tools.reflect.Eval
//    val tt = implicitly[c.WeakTypeTag[T]]
//    val ts = tt.tpe.typeSymbol
//    val fs = f.staticType
//    val fa = f.actualType
    val msg = c.Expr[String](Literal(Constant(show(f.tree))))
    println(s"In memoize_impl: ${f.tree}")
    val r = reify { () => {val result = f.splice() ; println(s"${msg.splice} = $result") ; result} }
    r
  }

//  implicit class MemoizeFunction[T](f: () => T) {
//    def memoized = (macro memoize_impl[T])(f)
//  }


}
