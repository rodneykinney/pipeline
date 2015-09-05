package org.allenai.pipeline

import org.allenai.common.Resource

import org.apache.commons.io.IOUtils
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.{ClassReader, ClassVisitor, MethodVisitor, Type}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map => MMap, Set}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

object FunctionConverter {

  def stepInfoFor(func: AnyRef): PipelineStepInfo = {
    val classContents = getClassFileContents(func.getClass)
    val versionId = (classContents.foldLeft(0L) { (hash, char) => hash * 31 + char}).toHexString

    PipelineStepInfo(
      className = func.getClass.getName,
      classVersion = versionId
    )
  }

  def getClassReader(cls: Class[_]): ClassReader = {
    getClassFileContents(cls) match {
      case null => new ClassReader(null: InputStream)
      case b => new ClassReader(new ByteArrayInputStream(b))
    }
  }

  private def getClassFileContents(cls: Class[_]): Array[Byte] = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    // todo: Fixme - continuing with earlier behavior ...
    if (resourceStream == null) return null

    val baos = new ByteArrayOutputStream(128)
    Resource.using(resourceStream) { is =>
      Resource.using(baos) { os =>
        IOUtils.copy(is, os)
      }
    }
    baos.toByteArray
  }

  def logDebug(s: String) = () //println(s)

  private def instantiateClass(
    cls: Class[_],
    enclosingObject: AnyRef
    ): AnyRef = {
    // This is a bona fide closure class, whose constructor has no effects
    // other than to set its fields, so use its constructor
    val cons = cls.getConstructors()(0)
    val params = cons.getParameterTypes.map(createNullValue).toArray
    if (enclosingObject != null) {
      params(0) = enclosingObject // First param is always enclosing object
    }
    cons.newInstance(params: _*).asInstanceOf[AnyRef]
  }

  private def createNullValue(cls: Class[_]): AnyRef = {
    if (cls.isPrimitive) {
      new java.lang.Byte(0: Byte) // Should be convertible to any primitive type
    } else {
      null
    }
  }

  def findParameters(
    func: AnyRef): Map[String, Any] = {
    val accessedFields = MMap.empty[Class[_], Set[String]]
    val params = MMap.empty[String, Any]

    if (func == null) {
      return params.toMap
    }

    logDebug(s"+++ Cleaning closure $func (${func.getClass.getName}}) +++")

    // A list of classes that represents closures enclosed in the given one
    val innerClasses = getInnerClosureClasses(func)

    // Fail fast if we detect return statements in closures
    getClassReader(func.getClass).accept(new ReturnStatementFinder(), 0)

    // List of outer (class, object) pairs, ordered from outermost to innermost
    var outerPairs: List[(Class[_], AnyRef)] = getOuterClassesAndObjects(func)

    // If accessed fields is not populated yet, we assume that
    // the closure we are trying to clean is the starting one
    logDebug(s" + populating accessed fields because this is the starting closure")
    // Initialize accessed fields with the outer classes first
    // This step is needed to associate the fields to the correct classes later
    for (cls <- outerPairs.map(_._1) ++ innerClasses) {
      accessedFields(cls) = Set[String]()
    }
    // Populate accessed fields by visiting all fields and methods accessed by this and
    // all of its inner closures. If transitive cleaning is enabled, this may recursively
    // visits methods that belong to other classes in search of transitively referenced fields.
    for (cls <- func.getClass :: innerClasses) {
      getClassReader(cls).accept(new FieldAccessFinder(accessedFields), 0)
    }

    logDebug(s" + fields accessed by starting closure: " + accessedFields.size)
    accessedFields.foreach { f => logDebug("     " + f)}

    for ((cls, obj) <- outerPairs) {
      logDebug(s" + cloning the object $obj of class ${cls.getName}")
      // We null out these unused references by cloning each object and then filling in all
      // required fields from the original object. We need the parent here because the Java
      // language specification requires the first constructor parameter of any closure to be
      // its enclosing object.
      for (fieldName <- accessedFields(cls)) {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        val value = field.get(obj)
        require(isValidParameter(value), s"Closure references non-primitive value: $value")
        val nameCandidates = List(
          fieldName.takeWhile(_ != '$'),
          fieldName,
          s"$obj.$fieldName")
        nameCandidates.find(s => !params.contains(s)) match {
          case Some(name) => params(name) = value
          case None => sys.error(s"Multiple fields with same name: $fieldName")
        }
      }
    }
    params.toMap
  }

  def isValidParameter(x: Any): Boolean = {
    def contentsValid(contents: Iterator[_]) = {
      var valid = true
      while (valid && contents.hasNext) {
        valid = valid && isValidParameter(contents.next())
      }
      valid
    }
    x match {
      case u: Unit => true
      case z: Boolean => true
      case b: Byte => true
      case c: Char => true
      case s: Short => true
      case i: Int => true
      case j: Long => true
      case f: Float => true
      case d: Double => true
      case s: String => true
      case l: List[_] => contentsValid(l.iterator)
      case m: Map[_, _] => contentsValid(m.iterator)
      case p: Product =>
        var valid = true
        val members = p.productIterator
        while (valid && members.hasNext) {
          valid = valid && isValidParameter(members.next())
        }
        valid
      case _ => false
    }
  }

  /**
   * Return the fields accessed by the given closure by class.
   * This also optionally finds the fields transitively referenced through methods invocations.
   */
  def findAccessedFields(
    closure: AnyRef,
    outerClasses: Seq[Class[_]]): Map[Class[_], collection.immutable.Set[String]] = {
    val fields = new mutable.HashMap[Class[_], mutable.Set[String]]
    outerClasses.foreach { c => fields(c) = new mutable.HashSet[String]}
    getClassReader(closure.getClass)
      .accept(new FieldAccessFinder(fields), 0)
    fields.mapValues(_.toSet).toMap
  }


  /** Helper class to identify a method. */
  case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

  /** Find the fields accessed by a given class.
    *
    * The resulting fields are stored in the mutable map passed in through the constructor.
    * This map is assumed to have its keys already populated with the classes of interest.
    *
    * @param fields the mutable map that stores the fields to return
    * @param specificMethod if not empty, visit only this specific method
    * @param visitedMethods a set of visited methods to avoid cycles
    */
  class FieldAccessFinder(
    fields: MMap[Class[_], Set[String]],
    specificMethod: Option[MethodIdentifier[_]] = None,
    visitedMethods: Set[MethodIdentifier[_]] = Set.empty
    )
    extends ClassVisitor(ASM4) {

    override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
      ): MethodVisitor = {

      // If we are told to visit only a certain method and this is not the one, ignore it
      if (specificMethod.isDefined &&
        (specificMethod.get.name != name || specificMethod.get.desc != desc)) {
        return null
      }

      new MethodVisitor(ASM4) {
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
          if (op == GETFIELD) {
            for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
              fields(cl) += name
            }
          }
        }

        override def visitMethodInsn(op: Int, owner: String, name: String, desc: String) {
          if (!owner.contains("scala")) {
            1 + 1
          }
          for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
            // Check for calls a getter method for a variable in an interpreter wrapper object.
            // This means that the corresponding field will be accessed, so we should save it.
            if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
              fields(cl) += name
            }
            // Optionally visit other methods to find fields that are transitively referenced
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m
              FunctionConverter.getClassReader(cl).accept(
                new FieldAccessFinder(fields, Some(m), visitedMethods), 0
              )
            }
          }
        }
      }
    }
  }

  def getOuterClassesAndObjects(obj: AnyRef): List[(Class[_], AnyRef)] = {
    val buff = new ListBuffer[(Class[_], AnyRef)]()
    buff += ((obj.getClass, obj))
    if (isClosure(obj.getClass)) {
      for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
        f.setAccessible(true)
        val outer = f.get(obj)
        // The outer pointer may be null if we have cleaned this closure before
        buff ++= getOuterClassesAndObjects(outer)
      }
    }
    buff.toList
  }

  // Get a list of the classes of the outer objects of a given closure object, obj;
  // the outer objects are defined as any closures that obj is nested within, plus
  // possibly the class that the outermost closure is in, if any. We stop searching
  // for outer objects beyond that because cloning the user's object is probably
  // not a good idea (whereas we can clone closure objects just fine since we
  // understand how all their fields are used).
  private def getOuterClasses(obj: AnyRef): List[Class[_]] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          return f.getType :: getOuterClasses(outer)
        } else {
          return f.getType :: Nil // Stop at the first $outer that is not a closure
        }
      }
    }
    Nil
  }

  // Get a list of the outer objects for a given closure object.
  private def getOuterObjects(obj: AnyRef): List[AnyRef] = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          return outer :: getOuterObjects(outer)
        } else {
          return outer :: Nil // Stop at the first $outer that is not a closure
        }
      }
    }
    Nil
  }

  // Check whether a class represents a Scala closure
  def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }


  /** Return a list of classes that represent closures enclosed in the given closure object.
    */
  def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    var stack = List[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.head)
      stack = stack.tail
      val set = Set[Class[_]]()
      cr.accept(new InnerClosureFinder(set), 0)
      for (cls <- set -- seen) {
        seen += cls
        stack = cls :: stack
      }
    }
    (seen - obj.getClass).toList
  }

  private class ReturnStatementFinder extends ClassVisitor(ASM4) {
    override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
      if (name.contains("apply")) {
        new MethodVisitor(ASM4) {
          override def visitTypeInsn(op: Int, tp: String) {
            if (op == NEW && tp.contains("scala/runtime/NonLocalReturnControl")) {
              sys.error("Return statements not allowed")
            }
          }
        }
      } else {
        new MethodVisitor(ASM4) {}
      }
    }
  }

  private class InnerClosureFinder(output: Set[Class[_]]) extends ClassVisitor(ASM4) {
    var myName: String = null

    // TODO: Recursively find inner closures that we indirectly reference, e.g.
    //   val closure1 = () = { () => 1 }
    //   val closure2 = () => { (1 to 5).map(closure1) }
    // The second closure technically has two inner closures, but this finder only finds one

    override def visit(version: Int, access: Int, name: String, sig: String,
      superName: String, interfaces: Array[String]) {
      myName = name
    }

    override def visitMethod(access: Int, name: String, desc: String,
      sig: String, exceptions: Array[String]): MethodVisitor = {
      new MethodVisitor(ASM4) {
        override def visitMethodInsn(op: Int, owner: String, name: String,
          desc: String) {
          val argTypes = Type.getArgumentTypes(desc)
          if (op == INVOKESPECIAL && name == "<init>" && argTypes.length > 0
            && argTypes(0).toString.startsWith("L") // is it an object?
            && argTypes(0).getInternalName == myName) {
            output += Class.forName(
              owner.replace('/', '.'),
              false,
              Thread.currentThread.getContextClassLoader
            )
          }
        }
      }
    }
  }

}
