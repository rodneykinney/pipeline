package org.allenai.pipeline

object PipeFunctions {

  import scala.language.implicitConversions

  implicit def F0toP[T](func: () => T): ProducerBuilder[T] = {
    new ProducerBuilder0(func)
  }
}

trait ProducerBuilder[T] extends Producer[T] {
  protected[this] var _stepInfo: PipelineStepInfo = PipelineStepInfo("")
  def pipeTo[O2](func: T => O2) = new ProducerBuilder1(this, func)
  def withName(name: String) = {
    _stepInfo = _stepInfo.copy(className = name)
    this
  }
  def stepInfo = _stepInfo
}

class ProducerBuilder0[T](func: () => T) extends ProducerBuilder[T] {
  def create = func()
}

class ProducerBuilder1[I, O](input: Producer[I], func: I => O) extends ProducerBuilder[O] {
  def create = func(input.get)
}
