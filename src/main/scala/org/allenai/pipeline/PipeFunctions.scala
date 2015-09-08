package org.allenai.pipeline

object PipeFunctions {
  implicit class FunctionToProducer[T](p: Producer[T]) {
    def pipeTo[O](func: T => O): Producer[O] = {
      val funcInfo = new ClosureAnalyzer(func)
      funcInfo.checkNoBadReferences
      new Producer[O] {
        override def create = func(p.get)

        override def stepInfo =
          PipelineStepInfo(func.getClass.getName)
        .addParameters(funcInfo.parameters.toList: _*)
      }
    }
  }

  implicit def FtoP[T](func: () => T): Producer[T] = {
    val funcInfo = new ClosureAnalyzer(func)
    funcInfo.checkNoBadReferences
    new Producer[T] {
      def create = func()

      def stepInfo =
        PipelineStepInfo(func.getClass.getName)
      .addParameters(funcInfo.parameters.toList: _*)
    }
  }

}
