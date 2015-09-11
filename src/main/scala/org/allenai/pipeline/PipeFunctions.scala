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
    ) = {
      pipeline.persist(build, io)
    }
    def create(): T
    lazy val build = {
      val name = usage.firstExteriorMethod.map(_._2.name).getOrElse(impl.getClass.getName)
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
