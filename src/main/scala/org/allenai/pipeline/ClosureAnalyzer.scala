package org.allenai.pipeline

import java.io.ByteArrayOutputStream

import org.allenai.common.Resource
import org.apache.commons.io.IOUtils
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.tree._
import org.objectweb.asm.{ ClassReader, ClassVisitor, MethodVisitor, Type }

import scala.collection.JavaConverters._
import scala.collection.mutable.{ ListBuffer, Map => MMap, Set => MSet }
import scala.runtime.VolatileObjectRef

object ClosureAnalyzer {

  def getClassReader(cls: Class[_]): ClassReader =
    new ClassReader(getClassFileContents(cls))

  def getClassFileContents(cls: Class[_]): Array[Byte] = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    require(resourceStream != null, s"Could not find definition for class $cls")

    val byteOs = new ByteArrayOutputStream(128)
    Resource.using(resourceStream) { is =>
      Resource.using(byteOs) { os =>
        IOUtils.copy(is, os)
      }
    }
    byteOs.toByteArray
  }

  def logDebug(s: String) = () //println(s)

  def isNull(x: Any) =
    x match {
      case ref: VolatileObjectRef[_] =>
        ref.elem == null
      case null => true
      case _ => false
    }

  def isPrimitive(x: Any): Boolean = {
    def contentsValid(contents: Iterator[_]) = {
      var valid = true
      while (valid && contents.hasNext) {
        valid = valid && isPrimitive(contents.next())
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
      case l: List[_] if l.getClass.getName.startsWith("scala") => contentsValid(l.iterator)
      case m: Map[_, _] if m.getClass.getName.startsWith("scala") => contentsValid(m.iterator)
      case o: Option[_] if o.getClass.getName.startsWith("scala") => contentsValid(o.iterator)
      case p: Product =>
        var valid = true
        val members = p.productIterator
        while (valid && members.hasNext) {
          valid = valid && isPrimitive(members.next())
        }
        valid
      case other => false
    }
  }

  case class ClassUsage(transitive: Boolean, fieldsAccessed: Set[String] = Set(), methodsInvoked: Set[MethodId] = Set()) {
    def addFieldAccessed(name: String) = this.copy(fieldsAccessed = this.fieldsAccessed + name)
    def addMethodInvoked(id: MethodId) = this.copy(methodsInvoked = this.methodsInvoked + id)
  }
  case class MethodId(name: String, desc: String)

  class ClassUsageAnalyzer(
    usages: MMap[Class[_], ClassUsage],
    transitive: Class[_] => Boolean,
    handleMethod: MethodId => Boolean = m => true
  )
      extends ClassVisitor(ASM4) {

    private def loadClass(owner: String) = Class.forName(owner.replace('/', '.'), false, Thread.currentThread.getContextClassLoader)

    override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
    ): MethodVisitor = {

      // If we are told to visit only a certain method and this is not the one, ignore it
      if (!handleMethod(MethodId(name, desc))) {
        return null
      }

      new MethodVisitor(ASM4) {
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
          if (op == GETFIELD) {
            val cl = loadClass(owner)
            val usage = usages.getOrElseUpdate(cl, ClassUsage(transitive(cl)))
            usages(cl) = usage.addFieldAccessed(name)
          }
        }

        override def visitMethodInsn(op: Int, owner: String, name: String, desc: String) {
          if (!owner.startsWith("java/") && !owner.startsWith("scala/")) {
            val cl = loadClass(owner)
            val method = MethodId(name, desc)
            val visitedBefore = usages.get(cl).exists(_.methodsInvoked.contains(method))
            val usage = usages.getOrElseUpdate(cl, ClassUsage(transitive(cl)))
            usages(cl) = usage.addMethodInvoked(method)
            if (transitive(cl)) {
              // Optionally visit other methods to find fields that are transitively referenced
              if (!visitedBefore) {
                // Keep track of visited methods to avoid potential infinite cycles
                val visitor =
                  new ClassUsageAnalyzer(
                    usages,
                    transitive,
                    m => m == method
                  )
                ClosureAnalyzer.getClassReader(cl).accept(visitor, 0)
              }
            }
          }
        }
      }
    }
  }

  /** Helper class to identify a method. */
  case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

  class UsedFieldsFinder(
    trackedClasses: Set[Class[_]],
    handleMethod: (String, String) => Boolean = (a, b) => true,
    fields: MMap[Class[_], MSet[String]] = MMap.empty[Class[_], MSet[String]],
    visitedMethods: MSet[MethodIdentifier[_]] = MSet.empty,
    dependentClasses: MSet[Class[_]] = MSet.empty
  )
      extends ClassVisitor(ASM4) {

    private def loadClass(owner: String) = Class.forName(owner.replace('/', '.'), false, Thread.currentThread.getContextClassLoader)

    override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
    ): MethodVisitor = {

      // If we are told to visit only a certain method and this is not the one, ignore it
      if (!handleMethod(name, desc)) {
        return null
      }

      new MethodVisitor(ASM4) {
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String) {
          if (op == GETFIELD) {
            fields.getOrElseUpdate(loadClass(owner), MSet.empty[String]).add(name)
          }
        }

        override def visitMethodInsn(op: Int, owner: String, name: String, desc: String) {
          val cl = loadClass(owner)
          if (trackedClasses(cl)) {
            // Optionally visit other methods to find fields that are transitively referenced
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m
              val visitor =
                new UsedFieldsFinder(
                  trackedClasses,
                  (n: String, d: String) => n == name && d == desc,
                  fields,
                  visitedMethods,
                  dependentClasses
                )
              ClosureAnalyzer.getClassReader(cl).accept(visitor, 0)
            }
          } else if (!owner.startsWith("java/") && !owner.startsWith("scala/")) {
            dependentClasses += cl
          }
        }
      }
    }
  }

  def getOuterClassesAndObjects(obj: AnyRef): List[(Class[_], AnyRef)] = {
    val buff = new ListBuffer[(Class[_], AnyRef)]()
    if (isClosure(obj.getClass)) {
      buff += ((obj.getClass, obj))
      for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
        f.setAccessible(true)
        val outer = f.get(obj)
        // The outer pointer may be null if we have cleaned this closure before
        buff ++= getOuterClassesAndObjects(outer)
      }
    }
    buff.toList
  }

  // Check whether a class represents a Scala closure
  def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  /** Return a list of classes that represent closures enclosed in the given closure object.
    */
  def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = MSet[Class[_]](obj.getClass)
    var stack = List[Class[_]](obj.getClass)
    while (stack.nonEmpty) {
      val cr = getClassReader(stack.head)
      stack = stack.tail
      val set = MSet[Class[_]]()
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

  private class InnerClosureFinder(output: MSet[Class[_]]) extends ClassVisitor(ASM4) {
    var myName: String = null

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

  def classForInternalName(owner: String) = {
    Class.forName(
      owner.replace('/', '.'),
      false,
      Thread.currentThread.getContextClassLoader
    )
  }

  class UsageNode extends ClassNode(ASM4) {
    def findMethods(filter: MethodInsnNode => Boolean) = {
      findInsnsAllMethods.collect { case m: MethodInsnNode if filter(m) => m }
    }
    def insnsInMethod(node: MethodNode) = node.instructions.toArray.iterator

    def findInsnsAllMethods =
      methods.asScala.asInstanceOf[Iterable[MethodNode]].iterator
        .flatMap(insnsInMethod)
  }

}

class ClosureAnalyzer(val closure: AnyRef) {

  import org.allenai.pipeline.ClosureAnalyzer._

  require(isClosure(closure.getClass), s"${closure.getClass} is not an anonymous closure")

  val classInfo = MMap.empty[Class[_], UsageNode]
  def loadClass(cls: Class[_]) = {
    val node = new UsageNode()
    getClassReader(cls).accept(node, 0)
    classInfo(cls) = node
  }
  loadClass(closure.getClass)
  for (innerNode <- classInfo(closure.getClass).innerClasses.asScala.asInstanceOf[Iterable[InnerClassNode]]) {
    loadClass(classForInternalName(innerNode.name))
  }
  {
    var outer = classInfo(closure.getClass).outerClass
    while (outer != null && outer.contains("$anonfun$")) {
      val cls = classForInternalName(outer)
      loadClass(cls)
      outer = classInfo(cls).outerClass
    }
  }

  private val outerClassesAndObjects = getOuterClassesAndObjects(closure)

  def outerClosureObjects = outerClassesAndObjects.map(_._2)

  def outerClosureClasses = outerClassesAndObjects.map(_._1)

  val innerClosureClasses = getInnerClosureClasses(closure)

  val classUsages = {
    val transitive = outerClosureClasses.toSet ++ innerClosureClasses.toSet
    val usages: MMap[Class[_], ClassUsage] = MMap()
    val visitor = new ClassUsageAnalyzer(usages, transitive)
    for (inner <- innerClosureClasses) {
      getClassReader(inner).accept(visitor, 0)
    }
    getClassReader(closure.getClass).accept(visitor, 0)
    usages.toMap
  }

  def firstExteriorMethod: Option[(Class[_], MethodId)] = {
    def namedMethod(m: MethodInsnNode) =
      m.name != "<init>" && !m.name.startsWith("apply")
    val extMethods = for {
      cls <- innerClosureClasses.iterator ++ List(closure.getClass).iterator
      method <- classInfo(cls).findMethods(namedMethod)
    } yield {
      val owner = classForInternalName(method.owner)
      val name = method.name
      (owner, MethodId(name, method.desc))
    }
    if (extMethods.hasNext) Some(extMethods.next) else None
  }

  def firstExteriorMethod2 = {
    val m = for {
      (cls, ClassUsage(_, fields, methods)) <- classUsages if isExternalClass(cls)
    } yield {
      require(methods.size <= 1, s"Multiple methods invoked by $cls")
      (cls, methods.head)
    }
    m.headOption
  }

  val (fieldsReferenced, classesReferenced) = {
    val fields = MMap[Class[_], MSet[String]](closure.getClass -> MSet.empty[String])
    val trackedClasses = outerClosureClasses.toSet ++ innerClosureClasses.toSet
    val extClasses = MSet.empty[Class[_]]
    val visitor = new UsedFieldsFinder(
      fields = fields,
      trackedClasses = trackedClasses,
      dependentClasses = extClasses
    )
    for (inner <- innerClosureClasses) {
      getClassReader(inner).accept(visitor, 0)
    }
    getClassReader(closure.getClass).accept(visitor, 0)
    val used = fields.mapValues(_.toSet).toMap
    (used, extClasses.toSet)

  }
  val objectsReferenced = {
    val refs = MMap.empty[(Class[_], String), AnyRef]
    for ((cls, obj) <- outerClassesAndObjects) {
      for (fieldName <- fieldsReferenced.getOrElse(cls, Set())) {
        val field = cls.getDeclaredField(fieldName)
        field.setAccessible(true)
        val value = field.get(obj)
        refs((cls, fieldName)) = value
      }
    }
    refs.toMap
  }

  private def isExternalRef(value: AnyRef) = {
    outerClosureObjects.find(_ eq value).isEmpty
  }

  private def isExternalClass(cls: Class[_]) = outerClosureClasses.find(_ == cls).isEmpty

  private def parameterName(cls: Class[_], fieldName: String) = {
    if (fieldName.startsWith(cls.getName.replace('.', '$'))) {
      fieldName.drop(cls.getName.size).dropWhile(_ == '$').takeWhile(_ != '$')
    } else {
      fieldName.takeWhile(_ != '$')
    }
  }

  val (externalPrimitivesReferenced, externalNonPrimitivesReferenced) =
    objectsReferenced
      .filterNot { case ((cls, fieldName), value) => isNull(value) }
      .filter { case ((cls, fieldName), value) => isExternalRef(value) }
      .partition { case ((cls, fieldName), value) => isPrimitive(value) }
  val parameters = {
    val params = MMap.empty[String, Any]
    for (((cls, fieldName), value) <- externalPrimitivesReferenced) {
      val name = parameterName(cls, fieldName)
      require(!params.contains(name), s"Object $closure[${closure.getClass.getName}}] has multiple fields named '$name'")
      params(name) = value
    }
    params.toMap
  }

  def checkNoBadReferences() = {
    if (externalNonPrimitivesReferenced.nonEmpty) {
      val badReferences =
        for (((cls, fieldName), value) <- externalNonPrimitivesReferenced) yield {
          s"$fieldName='$value'"
        }
      sys.error(s"Closure $closure[${closure.getClass.getName}] has references to external non-primitive objects: {${badReferences.mkString("; ")}}. Should refer to local val's instead of class members")
    }
  }
}

