package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

class TestClosureAnalyzer extends UnitSpec {

  import org.allenai.pipeline.ClosureAnalyzer._

  // Some fields and methods to reference in inner closures later
  private val aPrimitiveValue = 1
  private var mutablePrimitive = 1
  private val aNonPrimitiveValue = new NonPrimitive

  private def methodReturningPrimitive() = 1

  private def methodReturningNonPrimitive() = new NonPrimitive

  "ClosureAnalyzer" should "analyze self-contained closures" in {
    val closure1 = () => 1
    val closure2 = () => { () => 1 }
    val closure3 = (i: Int) => {
      (1 to i).map { x => x + 1 }.filter { x => x > 5 }
    }
    val closure4 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).flatMap { y =>
          (1 to y).map { z => z + 1 }
        }
      }
    }
    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)
    val ca3 = new ClosureAnalyzer(closure3)
    val ca4 = new ClosureAnalyzer(closure4)

    assert(ca1.innerClosureClasses.isEmpty)
    assert(ca2.innerClosureClasses.size === 1)
    assert(ca3.innerClosureClasses.size === 2)
    assert(ca4.innerClosureClasses.size === 3)
    assert(ca1.innerClosureClasses.forall(isClosure))
    assert(ca2.innerClosureClasses.forall(isClosure))
    assert(ca3.innerClosureClasses.forall(isClosure))
    assert(ca4.innerClosureClasses.forall(isClosure))

    assert(ca1.outerClosureObjects.size === 1)
    assert(ca2.outerClosureObjects.size === 1)
    assert(ca3.outerClosureObjects.size === 1)
    assert(ca4.outerClosureObjects.size === 1)
    assert(ca1.outerClosureClasses.forall(isClosure))
    assert(ca2.outerClosureClasses.forall(isClosure))
    assert(ca3.outerClosureClasses.forall(isClosure))
    assert(ca4.outerClosureClasses.forall(isClosure))

    checkRefs(ca1)()()
    checkRefs(ca2)()()
    checkRefs(ca3)()()
    checkRefs(ca4)()()
  }

  it should "analyze closures with external references" in {
    val localValue = aPrimitiveValue
    val closure1 = () => 1
    val closure2 = () => localValue
    val closure3 = () => aPrimitiveValue
    val closure4 = () => methodReturningPrimitive()

    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)
    val ca3 = new ClosureAnalyzer(closure3)
    val ca4 = new ClosureAnalyzer(closure4)

    // These do not have $outer pointers because they reference only local variables
    assert(ca1.outerClosureObjects.size === 1)
    assert(ca2.outerClosureObjects.size === 1)
    // These closures do have $outer pointers
    // because they ultimately reference the closure that defines this test (see FunSuite#test)
    assert(ca3.outerClosureObjects.size === 2)
    assert(ca4.outerClosureObjects.size === 2)

    checkRefs(ca1)()()
    checkRefs(ca2)("localValue" -> localValue)()
    checkRefs(ca3)()(this)
    checkRefs(ca4)()(this)
  }

  it should "analyze closures defined within methods" in {
    val localValue = aPrimitiveValue

    val test1 = () => {
      val x = 1
      val closure1 = () => 1
      val closure2 = () => x
      val closure3 = () => localValue
      val closure4 = () => aPrimitiveValue

      checkRefs(new ClosureAnalyzer(closure1))()()
      checkRefs(new ClosureAnalyzer(closure2))("x" -> 1)()
      checkRefs(new ClosureAnalyzer(closure3))("localValue" -> aPrimitiveValue)()
      checkRefs(new ClosureAnalyzer(closure4))()(this)
    }

    test1()
  }

  it should "find parameters in basic closures" in {
    val localValue = aPrimitiveValue
    mutablePrimitive = 55
    val localValue2 = mutablePrimitive
    mutablePrimitive = 117
    val closure1 = () => 1
    val closure2 = () => Array[String]("a", "b", "c")
    val closure3 = (s: String, arr: Array[Long]) => s + arr.mkString(", ")
    val closure4 = () => localValue
    val closure5 = () => new NonPrimitive(5) // we're just serializing the class information
    val closure6 = () => localValue2

    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)
    val ca3 = new ClosureAnalyzer(closure3)
    val ca4 = new ClosureAnalyzer(closure4)
    val ca5 = new ClosureAnalyzer(closure5)
    val ca6 = new ClosureAnalyzer(closure6)

    checkRefs(ca1)()()
    checkRefs(ca2)()()
    checkRefs(ca3)()()
    checkRefs(ca4)("localValue" -> localValue)()
    checkRefs(ca5)()()
    checkRefs(ca6)("localValue2" -> localValue2)()

    checkClasses(ca1)()
    checkClasses(ca5)(classOf[NonPrimitive])
  }

  it should "detect non-primitive references in basic closures" in {
    val closure1 = () => this
    val closure2 = () => methodReturningNonPrimitive()
    val closure3 = () => methodReturningPrimitive()
    val closure4 = () => aNonPrimitiveValue
    val closure5 = () => aPrimitiveValue

    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)
    val ca3 = new ClosureAnalyzer(closure3)
    val ca4 = new ClosureAnalyzer(closure4)
    val ca5 = new ClosureAnalyzer(closure5)

    checkRefs(ca1)()(this)
    checkRefs(ca2)()(this)
    checkRefs(ca3)()(this)
    checkRefs(ca4)()(this)
    checkRefs(ca5)()(this)
  }

  it should "find references in basic closures" in {
    val localValue = aPrimitiveValue
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + localValue } // 1 level of nesting
    }
    val closure2 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).map { y => y + localValue } // 2 levels
      }
    }
    val closure3 = (k: Int, l: Int, m: Int) => {
      (1 to k).flatMap(closure2) ++ // 4 levels
        (1 to l).flatMap(closure1) ++ // 3 levels
        (1 to m).map { x => x + 1 } // 2 levels
    }
    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)
    val ca3 = new ClosureAnalyzer(closure3)

    checkRefs(ca1)("localValue" -> aPrimitiveValue)()
    checkRefs(ca2)("localValue" -> aPrimitiveValue)()
    checkRefs(ca3)()(closure1, closure2)
  }

  it should "find more references in basic closures" in {
    def localMethod(): Int = aPrimitiveValue
    val localNonPrimitive = aNonPrimitiveValue
    // These closures ultimately reference the test suite object
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + aPrimitiveValue }
    }
    val closure2 = (j: Int) => {
      (1 to j).map { x => x + methodReturningPrimitive() }
    }
    val closure4 = (k: Int) => {
      (1 to k).map { x => x + localMethod() }
    }
    // This closure references a local non-serializable value
    val closure3 = (l: Int) => {
      (1 to l).map { x => localNonPrimitive }
    }
    // Deeply nested reference
    val closure5 = (m: Int) => {
      (1 to m).foreach { x =>
        (1 to x).foreach { y =>
          (1 to y).foreach { z =>
            aPrimitiveValue
          }
        }
      }
    }

    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)
    val ca3 = new ClosureAnalyzer(closure3)
    val ca4 = new ClosureAnalyzer(closure4)
    val ca5 = new ClosureAnalyzer(closure5)

    checkRefs(ca1)()(this)
    checkRefs(ca2)()(this)
    checkRefs(ca3)()(localNonPrimitive)
    checkRefs(ca4)()(this)
    checkRefs(ca5)()(this)
  }

  it should "find references with different usage patterns" in {
    val asVal: (Int => Int) = ObjectWithMethod.apply
    val asAnonFunc = (x: Int) => ObjectWithMethod.apply(x)
    def asDef(x: Int) = ObjectWithMethod(x)

    val ca1 = new ClosureAnalyzer(ObjectWithMethod.apply _)
    val ca2 = new ClosureAnalyzer(asVal)
    val ca3 = new ClosureAnalyzer(asAnonFunc)
    val ca4 = new ClosureAnalyzer(asDef _)
    val ca5 = new ClosureAnalyzer(ObjectWithMethod.curriedMethod(55) _)
    val ca6 = new ClosureAnalyzer(ObjectWithMethod.handCurriedMethod(55))

    checkRefs(ca1)()()
    checkRefs(ca2)()()
    checkRefs(ca3)()()
    checkRefs(ca4)()()
    checkRefs(ca5)()()
    checkRefs(ca6)("delta" -> 55)()

    checkClasses(ca1)(ObjectWithMethod.getClass)
    checkClasses(ca2)(ObjectWithMethod.getClass)
    checkClasses(ca3)(ObjectWithMethod.getClass)
    checkClasses(ca4)(ObjectWithMethod.getClass)
    checkClasses(ca5)(ObjectWithMethod.getClass)
    checkClasses(ca6)()
  }

  it should "find parameters in functions of non-primitive arguments" in {
    val localValue = aPrimitiveValue
    val closure1 = (x: NonPrimitive) => x.id * 5
    val closure2 = (x: NonPrimitive) => x.id * 5 + localValue
    val closure3 = (x: NonPrimitive, y: Int) => x.id * y + localValue

    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)
    val ca3 = new ClosureAnalyzer(closure3)

    checkRefs(ca1)()()
    checkRefs(ca2)("localValue" -> localValue)()
    checkRefs(ca3)("localValue" -> localValue)()
    checkClasses(ca1)(classOf[NonPrimitive])
    checkClasses(ca2)(classOf[NonPrimitive])
    checkClasses(ca3)(classOf[NonPrimitive])
  }

  it should "find references in inner classes" in {
    val localValue = aPrimitiveValue
    val expected = "localValue" -> aPrimitiveValue

    object ObjectWithMethod {
      def apply(i: Int) = i + localValue
    }

    val ca1 = new ClosureAnalyzer(ObjectWithMethod.apply _)
    checkRefs(ca1)(expected)()
    checkClasses(ca1)(ObjectWithMethod.getClass)

    object ObjectExtendingFunction extends (Int => Int) {
      def apply(i: Int) = i + localValue
    }
    val ca2 = new ClosureAnalyzer(ObjectExtendingFunction)
    checkRefs(ca2)(expected)()
    checkClasses(ca2)()

    object ObjectWithUsedMember {
      val x = 55

      def apply(i: Int) = i + localValue + x
    }
    val ca3 = new ClosureAnalyzer(ObjectWithUsedMember.apply _)
    checkRefs(ca3)(expected)()
    checkClasses(ca3)(ObjectWithUsedMember.getClass)

    def curried(delta: Int)(x: Int, y: Int) = math.max(x, y) + delta
    checkRefs(new ClosureAnalyzer(curried(55) _))()()

    val np = new NonPrimitive(55)
    checkRefs(new ClosureAnalyzer(np.equals _))()(np)

  }

  it should "find references in complicated nested closures" in {
    val localValue = aPrimitiveValue
    // Reference local fields from all levels
    val closure1 = (i: Int) => {
      val a = 1
      (1 to i).flatMap { x =>
        val b = a + 1
        (1 to x).map { y =>
          y + a + b + localValue
        }
      }
    }

    // Reference local fields and methods from all levels within the outermost closure
    val closure2 = (i: Int) => {
      val a1 = 1
      def a2 = 2
      (1 to i).flatMap { x =>
        val b1 = a1 + 1
        def b2 = a2 + 1
        (1 to x).map { y =>
          y + a1 + a2 + b1 + b2 + localValue
        }
      }
    }

    val ca1 = new ClosureAnalyzer(closure1)
    val ca2 = new ClosureAnalyzer(closure2)

    checkRefs(ca1)("localValue" -> localValue)()
    checkRefs(ca2)("localValue" -> localValue)()

  }

  it should "find references in complicated deeply nested closures" in {
    val localValue = aPrimitiveValue

    // Note that we are not interested in cleaning the outer closures here (they are not cleanable)
    // The only reason why they exist is to nest the inner closures

    val test1 = () => {
      val a = localValue
      val b = Map("a" -> 1, "b" -> 2)
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      val ca1 = new ClosureAnalyzer(inner1)
      val ca2 = new ClosureAnalyzer(inner2)

      checkRefs(ca1)(
        "a" -> a,
        "b" -> b
      )()

      checkRefs(ca2)("a" -> a)()
    }

    // Same as above, but the `val a` becomes `def a`
    // The difference here is that all inner closures now have pointers to the outer closure
    val test2 = () => {
      def a = localValue
      val b = Map("a" -> 1, "b" -> 2)
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      val ca1 = new ClosureAnalyzer(inner1)
      val ca2 = new ClosureAnalyzer(inner2)

      checkRefs(ca1)(
        "localValue" -> localValue,
        "b" -> b
      )()

      checkRefs(ca2)("localValue" -> localValue)()
    }

    // Same as above, but with more levels of nesting
    val test3 = () => { () => test1() }
    val test4 = () => { () => test2() }
    val test5 = () => { () => { () => test3() } }
    val test6 = () => { () => { () => test4() } }

    test1()
    test2()
    test3()()
    test4()()
    test5()()()
    test6()()()
  }

  /** Helper method for testing whether closure cleaning works as expected. */
  private def checkRefs(ca: ClosureAnalyzer)(expectedPrimitives: (String, Any)*)(expectedRefs: AnyRef*) = {
    ca.parameters.toSet should equal(expectedPrimitives.toSet)
    ca.externalNonPrimitivesReferenced.map(_._2).toSet should equal(expectedRefs.toSet)
  }

  private def checkClasses(ca: ClosureAnalyzer)(expectedClasses: Class[_]*) =
    ca.classesReferenced should equal(expectedClasses.toSet)
}

class NonPrimitive(val id: Int = -1) {
  override def equals(other: Any): Boolean = {
    other match {
      case o: NonPrimitive => id == o.id
      case _ => false
    }
  }

  override def hashCode = id
}

object ObjectWithMethod {
  def apply(i: Int) = i + 55

  def curriedMethod(delta: Int)(x: Int) = x + delta

  def handCurriedMethod(delta: Int) = (x: Int) => x + delta
}

