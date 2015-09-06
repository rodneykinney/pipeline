package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

import org.objectweb.asm.ClassReader

import scala.util.matching.Regex

class TestFunctionConverter extends UnitSpec {
  // Some fields and methods to reference in inner closures later
  private val aPrimitiveValue = 1
  private var mutablePrimitive = 1
  private val aNonPrimitiveValue = new NonPrimitive

  private def methodReturningPrimitive() = 1

  private def methodReturningNonPrimitive() = new NonPrimitive

  "FunctionConverter" should "get inner closure classes" in {
    val closure1 = () => 1
    val closure2 = () => { () => 1}
    val closure3 = (i: Int) => {
      (1 to i).map { x => x + 1}.filter { x => x > 5}
    }
    val closure4 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).flatMap { y =>
          (1 to y).map { z => z + 1}
        }
      }
    }
    import org.allenai.pipeline.FunctionConverter._
    val inner1 = getInnerClosureClasses(closure1)
    val inner2 = getInnerClosureClasses(closure2)
    val inner3 = getInnerClosureClasses(closure3)
    val inner4 = getInnerClosureClasses(closure4)
    assert(inner1.isEmpty)
    assert(inner2.size === 1)
    assert(inner3.size === 2)
    assert(inner4.size === 3)
    assert(inner2.forall(isClosure))
    assert(inner3.forall(isClosure))
    assert(inner4.forall(isClosure))
  }

  it should "get outer classes and objects" in {
    import org.allenai.pipeline.FunctionConverter._
    val localValue = aPrimitiveValue
    val closure1 = () => 1
    val closure2 = () => localValue
    val closure3 = () => aPrimitiveValue
    val closure4 = () => methodReturningPrimitive()

    val (outerClasses1, outerObjects1) = getOuterClassesAndObjects(closure1).unzip
    val (outerClasses2, outerObjects2) = getOuterClassesAndObjects(closure2).unzip
    val (outerClasses3, outerObjects3) = getOuterClassesAndObjects(closure3).unzip
    val (outerClasses4, outerObjects4) = getOuterClassesAndObjects(closure4).unzip

    // These do not have $outer pointers because they reference only local variables
    assert(outerClasses1.size === 1)
    assert(outerClasses2.size === 1)

    // These closures do have $outer pointers because they ultimately reference `this`
    // The first $outer pointer refers to the closure defines this test (see FunSuite#test)
    // The second $outer pointer refers to ClosureCleanerSuite2
    assert(outerClasses3.size === 3)
    assert(outerClasses4.size === 3)
    assert(isClosure(outerClasses3(0)))
    assert(isClosure(outerClasses4(0)))
    assert(outerClasses3(1) === outerClasses4(1)) // part of the same "FunSuite#test" scope
    assert(outerClasses3(2) === this.getClass)
    assert(outerClasses4(2) === this.getClass)
    assert(outerObjects3(2) === this)
    assert(outerObjects4(2) === this)
  }

  it should "get outer classes and objects with nesting" in {
    val localValue = aPrimitiveValue
    import org.allenai.pipeline.FunctionConverter._

    val test1 = () => {
      val x = 1
      val closure1 = () => 1
      val closure2 = () => x
      val (outerClasses1, outerObjects1) = getOuterClassesAndObjects(closure1).unzip
      val (outerClasses2, outerObjects2) = getOuterClassesAndObjects(closure2).unzip

      // These inner closures only reference local variables, and so do not have $outer pointers
      assert(outerClasses1.size === 1)
      assert(outerClasses2.size === 1)
    }

    val test2 = () => {
      def y = 1
      val closure1 = () => 1
      val closure2 = () => y
      val closure3 = () => localValue
      val (outerClasses1, outerObjects1) = getOuterClassesAndObjects(closure1).unzip
      val (outerClasses2, outerObjects2) = getOuterClassesAndObjects(closure2).unzip
      val (outerClasses3, outerObjects3) = getOuterClassesAndObjects(closure3).unzip
      // Same as above, this closure only references local variables
      assert(outerClasses1.size === 1)
      // This closure references the "test2" scope because it needs to find the method `y`
      // Scope hierarchy: "test2" < "FunSuite#test" < ClosureCleanerSuite2
      assert(outerClasses2.size === 4)
      // This closure references the Suite#test scope because it needs to find the `localValue`
      // defined outside of this scope
      assert(outerClasses3.size === 4)
      assert(isClosure(outerClasses2(1)))
      assert(isClosure(outerClasses3(1)))
      assert(isClosure(outerClasses2(2)))
      assert(isClosure(outerClasses3(2)))
      assert(outerClasses2(1) === outerClasses3(1)) // part of the same "test2" scope
      assert(outerClasses2(2) === outerClasses3(2)) // part of the same "FunSuite#test" scope
      assert(outerClasses2(3) === this.getClass)
      assert(outerClasses3(3) === this.getClass)
      assert(outerObjects2(3) === this)
      assert(outerObjects3(3) === this)
    }

    test1()
    test2()
  }

  it should "find accessed fields" in {
    import org.allenai.pipeline.FunctionConverter._
    val localValue = aPrimitiveValue
    val closure1 = () => 1
    val closure2 = () => localValue
    val closure3 = () => aPrimitiveValue
    val (outerClasses1, _) = getOuterClassesAndObjects(closure1).unzip
    val (outerClasses2, _) = getOuterClassesAndObjects(closure2).unzip
    val (outerClasses3, _) = getOuterClassesAndObjects(closure3).unzip

    val fields1t = findAccessedFields(closure1, outerClasses1)
    val fields2t = findAccessedFields(closure2, outerClasses2)
    val fields3t = findAccessedFields(closure3, outerClasses3)

    assert(fields1t.size === 1)
    assert(fields1t(outerClasses1(0)).size == 0)

    assert(fields2t.size === 1)
    assert(fields2t(outerClasses2(0)).size == 1)
    assert(fields2t(outerClasses2(0)).head.startsWith("localValue"))

    // Because we find fields transitively now, we are able to detect that we need the
    // $outer pointer to get the field from the ClosureCleanerSuite2
    assert(fields3t.size === 3)
    assert(fields3t(outerClasses3(0)).size === 1)
    assert(fields3t(outerClasses3(0)).head === "$outer")
    assert(fields3t(outerClasses3(1)).size === 1)
    assert(fields3t(outerClasses3(1)).head === "$outer")
    assert(fields3t(outerClasses3(2)).size === 1)
    assert(fields3t(outerClasses3(2)).head.contains("aPrimitiveValue"))
  }

  it should "find accessed fields with nesting" in {
    import org.allenai.pipeline.FunctionConverter._
    val localValue = aPrimitiveValue

    val test1 = () => {
      def a = localValue + 1
      val closure1 = () => 1
      val closure2 = () => a
      val closure3 = () => localValue
      val closure4 = () => aPrimitiveValue
      val (outerClasses1, _) = getOuterClassesAndObjects(closure1).unzip
      val (outerClasses2, _) = getOuterClassesAndObjects(closure2).unzip
      val (outerClasses3, _) = getOuterClassesAndObjects(closure3).unzip
      val (outerClasses4, _) = getOuterClassesAndObjects(closure4).unzip

      // Now do the same, but find fields that the closures transitively reference
      val fields1t = findAccessedFields(closure1, outerClasses1)
      val fields2t = findAccessedFields(closure2, outerClasses2)
      val fields3t = findAccessedFields(closure3, outerClasses3)
      val fields4t = findAccessedFields(closure4, outerClasses4)
      assert(fields1t.size === 1)
      assert(fields2t.size === 4)
      assert(fields2t(outerClasses2(1)).size === 1) // `def a` references `localValue`
      assert(fields2t(outerClasses2(1)).head.contains("localValue"))
      assert(fields2t(outerClasses2(2)).isEmpty)
      assert(fields2t(outerClasses2(3)).isEmpty)
      assert(fields3t.size === 4)
      assert(fields3t(outerClasses3(1)).size === 1) // as before
      assert(fields3t(outerClasses3(1)).head.contains("localValue"))
      assert(fields3t(outerClasses3(2)).isEmpty)
      assert(fields3t(outerClasses3(3)).isEmpty)
      assert(fields4t.size === 4)
      // Through a series of method calls, we are able to detect that we ultimately access
      // ClosureCleanerSuite2's field `someSerializableValue`. Along the way, we also accessed
      // a few $outer parent pointers to get to the outermost object.
      assert(fields4t(outerClasses4(1)) === Set("$outer"))
      assert(fields4t(outerClasses4(2)) === Set("$outer"))
      assert(fields4t(outerClasses4(3)).size === 1)
      assert(fields4t(outerClasses4(3)).head.contains("aPrimitiveValue"))
    }

    test1()
  }

  /** Helper method for testing whether closure cleaning works as expected. */
  private def checkParameters(closure: AnyRef, expected: (String, Any)*): Unit = {
    val FunctionDecomposition(result, _) = FunctionConverter.findExternalReferences(closure)
    result.size should equal(expected.size)
    def findMatchingParam(rex: Regex) = result.collect { case (k, v) if rex.pattern.matcher(k).matches => v}.headOption
    for ((name, value) <- expected) {
      val foundValue = result.get(name)
      assert(foundValue.nonEmpty, s"No parameter with name matching $name")
      assert(foundValue.get === value, s"Expected parameter ($name, $value) not found")
    }
  }

  private def checkInvalidParameters(closure: AnyRef) = {
    an[Exception] should be thrownBy {
      checkParameters(closure)
    }
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

    checkParameters(closure1)
    checkParameters(closure2)
    checkParameters(closure3)
    checkParameters(closure4, "localValue" -> localValue)
    checkParameters(closure5)
    checkParameters(closure6, "localValue2" -> localValue2)
  }

  it should "detect invalid parameters in basic closures" in {
    val closure1 = () => this
    val closure2 = () => methodReturningNonPrimitive()
    val closure3 = () => methodReturningPrimitive()
    val closure4 = () => aNonPrimitiveValue
    val closure5 = () => aPrimitiveValue

    // These have invalid parameters because they ultimately refer to the test suite object
    checkInvalidParameters(closure1)
    checkInvalidParameters(closure2)
    checkInvalidParameters(closure3)
    checkInvalidParameters(closure4)
    checkInvalidParameters(closure5)
  }

  it should "find parameters in nested closures" in {
    val localValue = aPrimitiveValue
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + localValue} // 1 level of nesting
    }
    val closure2 = (j: Int) => {
      (1 to j).flatMap { x =>
        (1 to x).map { y => y + localValue} // 2 levels
      }
    }
    val closure3 = (k: Int, l: Int, m: Int) => {
      (1 to k).flatMap(closure2) ++ // 4 levels
        (1 to l).flatMap(closure1) ++ // 3 levels
        (1 to m).map { x => x + 1} // 2 levels
    }

    val expected = "localValue" -> aPrimitiveValue
    checkParameters(closure1, expected)
    checkParameters(closure2, expected)
    checkInvalidParameters(closure3)
  }

  it should "find parameters in classes" in {
    val localValue = aPrimitiveValue
    object ObjectWithMethod {
      def apply(i: Int) = i + localValue
    }

    val expected = "localValue" -> aPrimitiveValue

    checkParameters(ObjectWithMethod.apply _, expected)
    var asVal: (Int => Int) = ObjectWithMethod.apply
    checkParameters(asVal, expected)
    val asAnonFunc = (x: Int) => ObjectWithMethod.apply(x)
    checkParameters(asAnonFunc, expected)
    def asDef(x: Int) = ObjectWithMethod(x)
    checkParameters(asDef _, expected)

    object ObjectExtendingFunction extends (Int => Int) {
      def apply(i: Int) = i + localValue
    }
    checkParameters(ObjectExtendingFunction, expected)

    object ObjectWithUsedMember {
      val x = 55

      def apply(i: Int) = i + localValue + x
    }
    checkParameters(ObjectWithUsedMember.apply _, expected)

    object ObjectWithUnusedMember {
      val x = 55

      def apply(i: Int) = i + localValue
    }
    checkParameters(ObjectWithUnusedMember.apply _, expected)

    def curried(delta: Int)(x: Int, y: Int) = math.max(x, y) + delta
    checkParameters(curried(55) _)
    checkParameters(curried(55) _)

    def classBytes(func: AnyRef) = FunctionConverter.getClassFileContents(func.getClass)
    def versionId(obj: AnyRef) = (classBytes(obj).foldLeft(0L) { (hash, char) => hash * 31 + char}).toHexString
  }

  it should "detect invalid parameters in basic nested closures" in {
    def localSerializableMethod(): Int = aPrimitiveValue
    val localNonSerializableValue = aNonPrimitiveValue
    // These closures ultimately reference the test suite object
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + aPrimitiveValue}
    }
    val closure2 = (j: Int) => {
      (1 to j).map { x => x + methodReturningPrimitive()}
    }
    val closure4 = (k: Int) => {
      (1 to k).map { x => x + localSerializableMethod()}
    }
    // This closure references a local non-serializable value
    val closure3 = (l: Int) => {
      (1 to l).map { x => localNonSerializableValue}
    }
    // This is non-serializable no matter how many levels we nest it
    val closure5 = (m: Int) => {
      (1 to m).foreach { x =>
        (1 to x).foreach { y =>
          (1 to y).foreach { z =>
            aPrimitiveValue
          }
        }
      }
    }

    checkInvalidParameters(closure1)
    checkInvalidParameters(closure2)
    checkInvalidParameters(closure3)
    checkInvalidParameters(closure4)
    checkInvalidParameters(closure5)
  }

  it should "find parameters in complicated nested closures" in {
    val localValue = aPrimitiveValue

    // Here we assume that if the outer closure is serializable,
    // then all inner closures must also be serializable

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
          // If this references a method outside the outermost closure, then it will try to pull
          // in the ClosureCleanerSuite2. This is why `localValue` here must be a local `val`.
          y + a1 + a2 + b1 + b2 + localValue
        }
      }
    }

    checkParameters(closure1, "localValue" -> localValue)
    checkParameters(closure2, "localValue" -> localValue)
  }

  it should "detect invalid parameters in complicated deeply nested closures" in {
    val localValue = aPrimitiveValue

    // Note that we are not interested in cleaning the outer closures here (they are not cleanable)
    // The only reason why they exist is to nest the inner closures

    val test1 = () => {
      val a = localValue
      val b = Map("a" -> 1, "b" -> 2)
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // This closure explicitly references a non-serializable field
      // There is no way to clean it
      checkParameters(inner1,
        "a" -> localValue,
        "b" -> Map("a" -> 1, "b" -> 2))

      // This closure is serializable to begin with since it does not need a pointer to
      // the outer closure (it only references local variables)
      checkParameters(inner2, "a" -> localValue)
    }

    // Same as above, but the `val a` becomes `def a`
    // The difference here is that all inner closures now have pointers to the outer closure
    val test2 = () => {
      def a = localValue
      val b = Map("a" -> 1, "b" -> 2)
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // As before, this closure is neither serializable nor cleanable
      checkInvalidParameters(inner1)

      // This closure is no longer serializable because it now has a pointer to the outer closure,
      // which is itself not serializable because it has a pointer to the ClosureCleanerSuite2.
      // If we do not clean transitively, we will not null out this indirect reference.
      checkInvalidParameters(inner2)
    }

    // Same as above, but with more levels of nesting
    val test3 = () => { () => test1()}
    val test4 = () => { () => test2()}
    val test5 = () => { () => { () => test3()}}
    val test6 = () => { () => { () => test4()}}

    test1()
    test2()
    test3()()
    test4()()
    test5()()()
    test6()()()
  }

  it should "find implementation definition" in {
    import FunctionConverter._
    val closure1 = () => 55
    val closure2 = () => 55
    val contents1 = stripClassName(getClassFileContents(closure1.getClass))
    val contents2 = stripClassName(getClassFileContents(closure2.getClass))

    contents1 should equal(contents2)
  }

}

class NonPrimitive(val id: Int = -1) {
  override def equals(other: Any): Boolean = {
    other match {
      case o: NonPrimitive => id == o.id
      case _ => false
    }
  }
}

object ObjectWithMethod {
  //  def apply(i: Int) = i + localValue
}

object Function extends (Int => Int) {
  def apply(i: Int) = i + 55
}

object ObjectWithMember {
  val x = 55
}
