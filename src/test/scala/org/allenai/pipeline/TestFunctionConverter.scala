package org.allenai.pipeline

import org.allenai.common.testkit.UnitSpec

import scala.collection.mutable
import scala.util.matching.Regex

class TestFunctionConverter extends UnitSpec {
  // Some fields and methods to reference in inner closures later
  private val someSerializableValue = 1
  private val someNonSerializableValue = new NonSerializable

  private def someSerializableMethod() = 1

  private def someNonSerializableMethod() = new NonSerializable

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
    val localValue = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => localValue
    val closure3 = () => someSerializableValue
    val closure4 = () => someSerializableMethod()

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
    val localValue = someSerializableValue
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
    val localValue = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => localValue
    val closure3 = () => someSerializableValue
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
    assert(fields3t(outerClasses3(2)).head.contains("someSerializableValue"))
  }

  it should "find accessed fields with nesting" in {
    import org.allenai.pipeline.FunctionConverter._
    val localValue = someSerializableValue

    val test1 = () => {
      def a = localValue + 1
      val closure1 = () => 1
      val closure2 = () => a
      val closure3 = () => localValue
      val closure4 = () => someSerializableValue
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
      assert(fields4t(outerClasses4(3)).head.contains("someSerializableValue"))
    }

    test1()
  }

  val x = """\s+""".r

  /** Helper method for testing whether closure cleaning works as expected. */
  private def checkParameters(closure: AnyRef, expected: (Regex, Any)*): Unit = {
    val result = FunctionConverter.findParameters(closure)
    result.size should equal(expected.size)
    def findMatchingParam(rex: Regex) = result.collect{case (k,v) if rex.pattern.matcher(k).matches => v}.headOption
    for ((rex, value) <- expected) {
      val foundValue = findMatchingParam(rex)
      assert(foundValue.nonEmpty,s"No parameter with name matching $rex")
      assert(foundValue.get === value, s"Expected parameter ($rex, $value) not found")
    }
  }

  it should "clean basic serializable closures" in {
    val localValue = someSerializableValue
    val closure1 = () => 1
    val closure2 = () => Array[String]("a", "b", "c")
    val closure3 = (s: String, arr: Array[Long]) => s + arr.mkString(", ")
    val closure4 = () => localValue
    val closure5 = () => new NonSerializable(5) // we're just serializing the class information

    checkParameters(closure1)
    checkParameters(closure2)
    checkParameters(closure3)
    checkParameters(closure4, """.*localValue.*""".r -> someSerializableValue)
    checkParameters(closure5)
  }

  it should "clean basic non-serializable closures" in {
    val closure1 = () => this // ClosureCleanerSuite2 is not serializable
    val closure2 = () => someNonSerializableMethod()
    val closure3 = () => someSerializableMethod()
    val closure4 = () => someNonSerializableValue
    val closure5 = () => someSerializableValue

    // These are not cleanable because they ultimately reference the ClosureCleanerSuite2
    checkParameters(closure1, """.*\$outer""".r -> this)
    checkParameters(closure2, """.*\$outer""".r -> this)
    checkParameters(closure3, """.*\$outer""".r -> this)
    checkParameters(closure4, """.*\$outer""".r -> this, """.*NonSerializable.*""".r -> someNonSerializableMethod())
    checkParameters(closure5, """.*\$outer""".r -> this, """.*SerializableValue.*""".r -> someSerializableValue)
  }

  it should "clean basic nested serializable closures" in {
    val localValue = someSerializableValue
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
    val closure1r = closure1(1)
    val closure2r = closure2(2)
    val closure3r = closure3(3, 4, 5)

    checkParameters(closure1, """.*\.localValue.*""".r -> someSerializableValue)
    checkParameters(closure2, """.*\.localValue.*""".r -> someSerializableValue)
    checkParameters(closure3)

    // Verify that closures can still be invoked and the result still the same
    assert(closure1(1) === closure1r)
    assert(closure2(2) === closure2r)
    assert(closure3(3, 4, 5) === closure3r)
  }

  it should "clean basic nested non-serializable closures" in {
    def localSerializableMethod(): Int = someSerializableValue
    val localNonSerializableValue = someNonSerializableValue
    // These closures ultimately reference the ClosureCleanerSuite2
    // Note that even accessing `val` that is an instance variable involves a method call
    val closure1 = (i: Int) => {
      (1 to i).map { x => x + someSerializableValue}
    }
    val closure2 = (j: Int) => {
      (1 to j).map { x => x + someSerializableMethod()}
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
            someSerializableValue
          }
        }
      }
    }

    checkParameters(closure1)
    checkParameters(closure2)
    checkParameters(closure3)
    checkParameters(closure4)
    checkParameters(closure5)
  }

  it should "clean complicated nested serializable closures" in {
    val localValue = someSerializableValue

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

    val closure1r = closure1(1)
    val closure2r = closure2(2)
    checkParameters(closure1)
    checkParameters(closure2)
    assert(closure1(1) == closure1r)
    assert(closure2(2) == closure2r)
  }

  it should "clean complicated nested non-serializable closures" in {
    val localValue = someSerializableValue

    // Note that we are not interested in cleaning the outer closures here (they are not cleanable)
    // The only reason why they exist is to nest the inner closures

    val test1 = () => {
      val a = localValue
      val b = Map("a" -> 1, "b" -> 2)
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // This closure explicitly references a non-serializable field
      // There is no way to clean it
      checkParameters(inner1)

      // This closure is serializable to begin with since it does not need a pointer to
      // the outer closure (it only references local variables)
      checkParameters(inner2)
    }

    // Same as above, but the `val a` becomes `def a`
    // The difference here is that all inner closures now have pointers to the outer closure
    val test2 = () => {
      def a = localValue
      val b = Map("a" -> 1, "b" -> 2)
      val inner1 = (x: Int) => x + a + b.hashCode()
      val inner2 = (x: Int) => x + a

      // As before, this closure is neither serializable nor cleanable
      checkParameters(inner1)

      // This closure is no longer serializable because it now has a pointer to the outer closure,
      // which is itself not serializable because it has a pointer to the ClosureCleanerSuite2.
      // If we do not clean transitively, we will not null out this indirect reference.
      checkParameters(inner2)

      // If we clean transitively, we will find that method `a` does not actually reference the
      // outer closure's parent (i.e. the ClosureCleanerSuite), so we can additionally null out
      // the outer closure's parent pointer. This will make `inner2` serializable.
      checkParameters(inner2)
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

}

class NonSerializable(val id: Int = -1) {
  override def equals(other: Any): Boolean = {
    other match {
      case o: NonSerializable => id == o.id
      case _ => false
    }
  }
}
