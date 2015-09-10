package org.allenai.pipeline

import scala.reflect.ClassTag

object PipeFunctions {

  import scala.language.implicitConversions

  implicit def Build[T](builder: ProducerBuilder[T]): Producer[T] = builder.build

  trait ProducerBuilder[T] {
    outer =>
    protected[this] var info: PipelineStepInfo = PipelineStepInfo("")
    val impl: AnyRef
    lazy val usage = new ClosureAnalyzer(impl)
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
      usage
      val localInfo = info
      new Producer[T] {
        override def create = outer.create()
        override def stepInfo = localInfo
      }
    }
  }

  implicit class ProducerBuilder0[T](func: () => T) {
    def withNoInputs = new ProducerBuilder[T] {
      val impl = func
      info =
        info.copy(className = func.getClass.getName)
      def create() = func()
    }
  }

  implicit class ProducerBuilder1[I, O](func: I => O) {
    def withInput(input: Producer[I]) = {
      new ProducerBuilder[O] {
        val impl = func
        info =
          info.copy(className = func.getClass.getName)
            .addParameters("input" -> input)
        override def create() = func(input.get)
      }
    }
  }
  implicit class ProducerBuilder2[I1, I2, O](func: (I1, I2) => O) {
    def withInputs(
      input1: Producer[I1],
      input2: Producer[I2]
    ) = {
      new ProducerBuilder[O] {
        val impl = func
        info =
          info.copy(className = func.getClass.getName)
            .addParameters("input1" -> input1, "input2" -> input2)
        override def create() = func(input1.get, input2.get)
      }
    }
  }
}
