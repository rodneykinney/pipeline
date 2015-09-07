package org.allenai.pipeline

import org.allenai.common.Resource

import org.apache.commons.io.IOUtils
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.{ClassReader, ClassVisitor, MethodVisitor, Type}

import scala.collection.mutable.{ListBuffer, Map => MMap, Set => MSet}
import scala.runtime.VolatileObjectRef

import java.io.ByteArrayOutputStream
import java.nio.charset.Charset

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
      case x => false
    }
  }

  /** Helper class to identify a method. */
  case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

  class UsedFieldsFinder(
    trackedClasses: Set[Class[_]],
    handleMethod: (String, String) => Boolean = (a, b) => true,
    fields: MMap[Class[_], MSet[String]] = MMap.empty[Class[_], MSet[String]],
    visitedMethods: MSet[MethodIdentifier[_]] = MSet.empty,
    untrackedClassesReferenced: MSet[Class[_]] = MSet.empty
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
          if (!owner.contains("scala")) {
            1 + 1
          }
          val cl = loadClass(owner)
          if (trackedClasses(cl)) {
            //            fields.getOrElseUpdate(cl, MSet.empty[String])
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
                  untrackedClassesReferenced)
              ClosureAnalyzer.getClassReader(cl).accept(visitor, 0
              )
            }
          }
          else {
            untrackedClassesReferenced += cl
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
    while (!stack.isEmpty) {
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

  def stripClassName(contents: Array[Byte]): Array[Byte] = {
    val reader = new ClassReader(contents)
    //    reader.accept(new FieldAccessFinder(MMap.empty[Class[_], MSet[String]]), 0)
    reader.accept(new ClassVisitor(ASM4) {}, 0)
    val buff = new Array[Char](500)
    val className = reader.readClass(reader.header + 2, buff)
    val itemIndex1 = reader.readUnsignedShort(reader.header + 2)
    val cn2 = reader.readUTF8(reader.getItem(itemIndex1), buff)
    val itemIndex2 = reader.readUnsignedShort(reader.getItem(itemIndex1))
    val classNameLength = reader.readUnsignedShort(reader.getItem(itemIndex2))
    val classNameStart = reader.getItem(itemIndex2) + 2
    val classNameBytes = contents.drop(classNameStart).take(classNameLength)
    val verifyClassName = new String(classNameBytes, Charset.forName("UTF8"))
    contents.take(classNameStart) ++ contents.drop(classNameStart + classNameLength)
  }
}

class ClosureAnalyzer(obj: AnyRef) {

  import org.allenai.pipeline.ClosureAnalyzer._

  require(isClosure(obj.getClass), s"${obj.getClass} is not an anonymous closure")


  private val outerClassesAndObjects = getOuterClassesAndObjects(obj)

  def outerClosureObjects = outerClassesAndObjects.map(_._2)

  def outerClosureClasses = outerClassesAndObjects.map(_._1)

  val innerClosureClasses = getInnerClosureClasses(obj)
  val (fieldsReferenced, otherClassesUsed) = {
    val outerClasses = outerClassesAndObjects.map(_._1)
    val innerClasses = innerClosureClasses
    val fields = MMap[Class[_], MSet[String]](obj.getClass -> MSet.empty[String])
    val trackedClasses = outerClasses.toSet ++ innerClasses.toSet
    val extClasses = MSet.empty[Class[_]]
    val visitor = new UsedFieldsFinder(
      fields = fields,
      trackedClasses = trackedClasses,
      untrackedClassesReferenced = extClasses)
    for (inner <- innerClasses) {
      getClassReader(inner).accept(new UsedFieldsFinder(fields = fields, trackedClasses = trackedClasses, untrackedClassesReferenced = extClasses), 0)
    }
    getClassReader(obj.getClass).accept(visitor, 0)
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

  private def isExternalRef(value: AnyRef) = outerClosureObjects.find(_ eq value).isEmpty

  val (externalPrimitivesReferenced, externalNonPrimitivesReferenced) =
    objectsReferenced
      .filterNot { case ((cls, fieldName), value) => isNull(value)}
      .filter { case ((cls, fieldName), value) => isExternalRef(value)}
      .partition { case ((cls, fieldName), value) => isPrimitive(value)}
  val parameters = {
//    if (externalNonPrimitivesReferenced.nonEmpty) {
//      val badReferences =
//        (for (((cls, fieldName), value) <- externalNonPrimitivesReferenced) yield {
//          s"$fieldName='$value'"
//        }).mkString("; ")
//      sys.error(s"Closure $obj[${obj.getClass.getName}}] references non-primitive values: $badReferences")
//    }
    val params = MMap.empty[String, Any]
    for (((cls, fieldName), value) <- externalPrimitivesReferenced) {
      val name = fieldName.takeWhile(_ != '$')
      require(!params.contains(name), s"Object [$obj] has multiple fields named '$name'")
      params(name) = value
    }
    params.toMap
  }
}

