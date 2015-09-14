package org.allenai.pipeline

import org.allenai.common.Resource

import org.apache.commons.io.IOUtils
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.tree._
import org.objectweb.asm.{ ClassReader, ClassVisitor, MethodVisitor, Type }

import scala.collection.JavaConverters._
import scala.collection.mutable.{ ListBuffer, Map => MMap, Set => MSet }
import scala.runtime.VolatileObjectRef

import java.io.ByteArrayOutputStream

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

  /** Helper class to identify a method. */
  case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

  case class MethodId(owner: String, name: String, desc: String)

  object MethodId {
    def apply(insn: MethodInsnNode) =
      new MethodId(insn.owner, insn.name, insn.desc)

    def apply(owner: String, method: MethodNode) =
      new MethodId(owner, method.name, method.desc)
  }

  // Check whether a class represents a Scala closure
  def isClosure(cls: Class[_]): Boolean = {
    cls.getName.contains("$anonfun$")
  }

  /** Return a list of classes that represent closures enclosed in the given closure object.
    */
  def getInnerClosureClasses(cls: Class[_]): List[Class[_]] = {
    val seen = MSet[Class[_]](cls)
    var stack = List[Class[_]](cls)
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
    (seen - cls).toList
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

  class ClosureNode extends ClassNode(ASM4) {
    def insnNodesIn(node: MethodNode) = node.instructions.toArray

    def methodNodes = methods.asScala.asInstanceOf[Iterable[MethodNode]]

    def findInsnsAllMethods = methodNodes.iterator.flatMap(insnNodesIn)

    // Return Some(method) if this is a wrapper around a single method,
    // e.g. val closure = System.out.println _
    // A wrapper will have only "apply" methods, each of which has a single methodInsn
    def singleWrappedMethod = {
      def namedMethod(name: String) = name != "<init>" && !name.startsWith("apply")
      val unnamedMethods = methodNodes.filterNot(m => namedMethod(m.name))
      if (unnamedMethods.size == methods.size()) {
        val externalMethodsInvoked =
          unnamedMethods.map(m => insnNodesIn(m).collect {
            case m: MethodInsnNode if !m.owner.startsWith(name) &&
              !m.owner.startsWith("java/")
              && !m.owner.startsWith("scala/") => m
          })
        if (externalMethodsInvoked.flatten.size == 1) {
          externalMethodsInvoked.flatten.headOption
        } else {
          None
        }
      } else {
        None
      }
    }

    def invokedBy(node: MethodNode) = insnNodesIn(node).collect { case m: MethodInsnNode => m }

    def delegateOf(node: MethodNode) = {
      val methodCalls = invokedBy(node)
      if (methodCalls.size == 1 && methodCalls.head.owner == name) {
        methodCalls.headOption
      } else {
        None
      }
    }

    lazy val coreLogicMethod = {
      val nonCore = MSet.empty[MethodId]
      val applyMethods = methodNodes.filter(_.name.startsWith("apply"))
      for (applyMethod <- applyMethods) {
        val invoked = invokedBy(applyMethod).map(MethodId.apply)
        if (invoked.size > 0 &&
          invoked.forall(m => m.owner == "scala/runtime/BoxesRunTime" ||
            (m.owner == name && m.name.startsWith("apply")))) {
          val (delegates, _) = invoked.partition(_.owner == name)
          delegates.size match {
            case 1 => nonCore += MethodId(name, applyMethod)
            case 0 =>
            case _ => sys.error(s"$applyMethod invoked multiple delegates: $delegates")
          }
        }
      }
      val core = applyMethods.map(m => MethodId(name, m)).filterNot(nonCore).toSet
      val coreMethods = methodNodes.filter(m => core(MethodId(name, m)))
      require(coreMethods.size == 1, s"Found multiple methods that don't delegate: $core")
      coreMethods.head
    }

    def referencesClass(cls: Class[_]) = {
      val owner = cls.getName.replace('.', '/')
      val refs = findInsnsAllMethods.collect {
        case m: MethodInsnNode if m.owner == owner => m
        case f: FieldInsnNode if f.owner == owner => f
      }
      refs.nonEmpty
    }

    def classesReferenced = findInsnsAllMethods.collect {
      case m: MethodInsnNode => classForInternalName(m.owner)
      case f: FieldInsnNode => classForInternalName(f.owner)
    }.toSet

  }

}

class ClosureAnalyzer(val closure: AnyRef) {

  import org.allenai.pipeline.ClosureAnalyzer._

  require(isClosure(closure.getClass), s"${closure.getClass} is not an anonymous closure")

  val classInfo = MMap.empty[Class[_], ClosureNode]

  private def loadClass(cls: Class[_]) = {
    val node = new ClosureNode()
    getClassReader(cls).accept(node, 0)
    classInfo(cls) = node
  }

  loadClass(closure.getClass)
  val closureInfo = classInfo(closure.getClass)
  closureInfo.coreLogicMethod
  val innerClosureClasses = getInnerClosureClasses(closure.getClass)
  innerClosureClasses.foreach(loadClass)

  {
    var outer = closureInfo.outerClass
    while (outer != null && outer.contains("$anonfun$")) {
      val cls = classForInternalName(outer)
      loadClass(cls)
      outer = classInfo(cls).outerClass
    }
  }

  def firstExteriorMethod: Option[MethodIdentifier[_]] =
    closureInfo.singleWrappedMethod.map { m =>
      MethodIdentifier(classForInternalName(m.owner), cleanMethodName(m.name), m.desc)
    }

  def isDelegate = {
    innerClosureClasses.size == 0 &&
      closureInfo.singleWrappedMethod.size == 1
  }

  def implementingMethod = closureInfo.coreLogicMethod

  def cleanMethodName(name: String) = {
    val index = name.indexOf("$anonfun$")
    if (index >= 0) {
      name.drop(index + 9).dropWhile(_ == '$').takeWhile(_ != '$')
    } else {
      name
    }
  }

  private def parameterName(cls: Class[_], fieldName: String) = {
    if (fieldName.startsWith(cls.getName.replace('.', '$'))) {
      fieldName.drop(cls.getName.size).dropWhile(_ == '$').takeWhile(_ != '$')
    } else {
      fieldName.takeWhile(_ != '$')
    }
  }

  val objectsReferenced = {
    for (
      f <- closure.getClass.getDeclaredFields if f.getName != "serialVersionUID"
    ) yield {
      f.setAccessible(true)
      (f.getName, f.get(closure))
    }
  }

  val classesReferenced = {
    val allClasses = innerClosureClasses.foldLeft(closureInfo.classesReferenced) {
      case (classes, innerClass) => classes ++ classInfo(innerClass).classesReferenced
    }
    allClasses.filter(c => c != closure.getClass && !c.getName.startsWith("java.") && !c.getName.startsWith("scala."))
  }

  val (externalPrimitivesReferenced, externalNonPrimitivesReferenced) =
    objectsReferenced.partition { case (name, value) => isPrimitive(value) }

  val parameters = {
    val params = MMap.empty[String, Any]
    for ((fieldName, value) <- externalPrimitivesReferenced) {
      val name = fieldName.takeWhile(_ != '$')
      require(!params.contains(name), s"Object $closure[${closure.getClass.getName}}] has multiple fields named '$name'")
      params(name) = value
    }
    params.toMap
  }

  def checkNoBadReferences() = {
    if (externalNonPrimitivesReferenced.nonEmpty) {
      val badReferences =
        for ((fieldName, value) <- externalNonPrimitivesReferenced) yield {
          s"$fieldName='$value'"
        }
      sys.error(s"Closure $closure[${closure.getClass.getName}] has references to external non-primitive objects: {${badReferences.mkString("; ")}}. For primitives, use local val's. For non-primitives use as function parameters.")
    }
  }
}

