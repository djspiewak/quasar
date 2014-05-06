package slamdata.engine

import scalaz._

import scalaz.std.vector._
import scalaz.std.list._
import scalaz.std.indexedSeq._
import scalaz.std.anyVal._

import scalaz.syntax.monad._

import SemanticError.{TypeError, MissingField, MissingIndex}
import NonEmptyList.nel
import Validation.{success, failure}

sealed trait Type { self =>
  import Type._
  import scalaz.std.option._
  import scalaz.syntax.traverse._

  final def & (that: Type) = Type.Product(this, that)

  final def | (that: Type) = Type.Coproduct(this, that)

  final def lub: Type = mapUp(self) {
    case x : Coproduct => x.flatten.reduce(Type.lub)
  }

  final def glb: Type = mapUp(self) {
    case x : Coproduct => x.flatten.reduce(Type.glb)
  }

  final def contains(that: Type): Boolean = Type.typecheck(self, that).fold(Function.const(false), Function.const(true))

  final def objectLike: Boolean = this match {
    case Const(value) => value.dataType.objectLike
    case AnonField(_) => true
    case NamedField(_, _) => true
    case x : Product => x.flatten.toList.exists(_.objectLike)
    case x : Coproduct => x.flatten.toList.forall(_.objectLike)
    case _ => false
  }  

  final def arrayType: Option[Type] = this match {
    case Const(value) => value.dataType.arrayType
    case AnonElem(tpe) => Some(tpe)
    case IndexedElem(_, tpe) => Some(tpe)
    case x : Product => x.flatten.toList.map(_.arrayType).sequenceU.map(types => types.reduce(Type.lub _))
    case x : Coproduct => x.flatten.toList.map(_.arrayType).sequenceU.map(types => types.reduce(Type.lub _))
    case _ => None
  }

  final def arrayLike: Boolean = this match {
    case Const(value) => value.dataType.arrayLike
    case AnonElem(_) => true
    case IndexedElem(_, _) => true
    case x : Product => x.flatten.toList.exists(_.arrayLike)
    case x : Coproduct => x.flatten.toList.forall(_.arrayLike)
    case _ => false
  }

  final def setLike: Boolean = this match {
    case Const(value) => value.dataType.setLike
    case Set(_) => true
    case _ => false
  }

  final def objectField(field: Type): ValidationNel[SemanticError, Type] = {
    if (Type.lub(field, Str) != Str) failure(nel(TypeError(Str, field), Nil))
    else (field, this) match {
      case (Const(Data.Str(field)), Const(Data.Obj(map))) => 
        map.get(field).map(data => success(Const(data))).getOrElse(failure(nel(MissingField(field), Nil)))

      case (Str, Const(Data.Obj(map))) => success(map.values.map(_.dataType).foldLeft[Type](Top)(Type.lub _))

      case (Str, AnonField(value)) => success(value)
      case (Const(Data.Str(_)), AnonField(value)) => success(value)

      case (Const(Data.Str(field)), NamedField(name, value)) if (field == name) => success(value)

      case (_, x : Product) => x.flatten.toList.map(_.objectField(field)).reduce(_ ||| _)
      case (_, x : Coproduct) => 
        implicit val lub = Type.TypeLubSemigroup
        x.flatten.toList.map(_.objectField(field)).reduce(_ +++ _)

      case _ => failure(nel(TypeError(AnyObject, this), Nil))
    }
  }

  final def arrayElem(index: Type): ValidationNel[SemanticError, Type] = {
    if (Type.lub(index, Int) != Int) failure(nel(TypeError(Int, index), Nil))
    else (index, this) match {
      case (Const(Data.Int(index)), Const(Data.Arr(arr))) => 
        arr.lift(index.toInt).map(data => success(Const(data))).getOrElse(failure(nel(MissingIndex(index.toInt), Nil)))

      case (Int, Const(Data.Arr(arr))) => success(arr.map(_.dataType).foldLeft[Type](Top)(Type.lub _))

      case (Int, AnonElem(value)) => success(value)
      case (Const(Data.Int(_)), AnonElem(value)) => success(value)
      
      case (Const(Data.Int(index1)), IndexedElem(index2, value)) if (index1.toInt == index2) => success(value)
      case (_, x : Product) => x.flatten.toList.map(_.arrayElem(index)).reduce(_ ||| _)
      case (_, x : Coproduct) => 
        implicit val lub = Type.TypeLubSemigroup
        x.flatten.toList.map(_.arrayElem(index)).reduce(_ +++ _)

      case _ => failure(nel(TypeError(AnyArray, this), Nil))
    }
  }
}

trait TypeInstances {  
  val TypeOrMonoid = new Monoid[Type] {
    def zero = Type.Top

    def append(v1: Type, v2: => Type) = (v1, v2) match {
      case (Type.Top, that) => that
      case (this0, Type.Top) => this0
      case _ => v1 | v2
    }
  }

  val TypeAndMonoid = new Monoid[Type] {
    def zero = Type.Top

    def append(v1: Type, v2: => Type) = (v1, v2) match {
      case (Type.Top, that) => that
      case (this0, Type.Top) => this0
      case _ => v1 & v2
    }
  }

  val TypeLubSemigroup = new Semigroup[Type] {
    def append(f1: Type, f2: => Type): Type = Type.lub(f1, f2)
  }

  implicit val TypeShow = new Show[Type] {
    override def show(v: Type) = Cord(v.toString) // TODO
  }
}

case object Type extends TypeInstances {
  private def fail[A](expected: Type, actual: Type, message: Option[String]): ValidationNel[TypeError, A] = 
    Validation.failure(NonEmptyList(TypeError(expected, actual, message)))

  private def fail[A](expected: Type, actual: Type): ValidationNel[TypeError, A] = fail(expected, actual, None)

  private def fail[A](expected: Type, actual: Type, msg: String): ValidationNel[TypeError, A] = fail(expected, actual, Some(msg))

  private def succeed[A](v: A): ValidationNel[TypeError, A] = Validation.success(v)

  def simplify(tpe: Type): Type = mapUp(tpe) {
    case x : Product => Product(x.flatten.toList.map(simplify _).filter(_ != Top).distinct)
    case x : Coproduct => Coproduct(x.flatten.toList.map(simplify _).distinct)
    case _ => tpe
  }

  def glb(left: Type, right: Type): Type = (left, right) match {
    case (left, right) if left == right => left

    case (left, right) if left contains right => left
    case (left, right) if right contains left => right

    case _ => Bottom
  }

  def lub(left: Type, right: Type): Type = (left, right) match {
    case (left, right) if left == right => left

    case (left, right) if left contains right => right
    case (left, right) if right contains left => left

    case _ => Top
  }

  def typecheck(expected: Type, actual: Type): ValidationNel[TypeError, Unit] = (expected, actual) match {
    case (expected, actual) if (expected == actual) => succeed(Unit)

    case (Top, actual) => succeed(Unit)

    case (Const(expected), actual) => typecheck(expected.dataType, actual)
    case (expected, Const(actual)) => typecheck(expected, actual.dataType)

    case (expected : Product, actual : Product) => typecheckPP(expected.flatten, actual.flatten)

    case (expected : Product, actual : Coproduct) => typecheckPC(expected.flatten, actual.flatten)

    case (expected : Coproduct, actual : Product) => typecheckCP(expected.flatten, actual.flatten)

    case (expected : Coproduct, actual : Coproduct) => typecheckCC(expected.flatten, actual.flatten)

    case (AnonField(expected), AnonField(actual)) => typecheck(expected, actual)

    case (AnonField(expected), NamedField(name, actual)) => typecheck(expected, actual)
    case (NamedField(name, expected), AnonField(actual)) => typecheck(expected, actual)

    case (NamedField(name1, expected), NamedField(name2, actual)) if (name1 == name2) => typecheck(expected, actual)

    case (AnonElem(expected), AnonElem(actual)) => typecheck(expected, actual)

    case (AnonElem(expected), IndexedElem(idx, actual)) => typecheck(expected, actual)
    case (IndexedElem(idx, expected), AnonElem(actual)) => typecheck(expected, actual)

    case (IndexedElem(idx1, expected), IndexedElem(idx2, actual)) if (idx1 == idx2) => typecheck(expected, actual)

    case (Set(expected), Set(actual)) => typecheck(expected, actual)

    case (expected, actual : Coproduct) => typecheckPC(expected :: Nil, actual.flatten)

    case (expected : Coproduct, actual) => typecheckCP(expected.flatten, actual :: Nil)

    case (expected, actual : Product) => typecheckPP(expected :: Nil, actual.flatten)

    case (expected : Product, actual) => typecheckPP(expected.flatten, actual :: Nil)

    case _ => fail(expected, actual)
  }

  def children(v: Type): List[Type] = v match {
    case Top => Nil
    case Bottom => Nil
    case Const(value) => value.dataType :: Nil
    case Null => Nil
    case Str => Nil
    case Int => Nil
    case Dec => Nil
    case Bool => Nil
    case Binary => Nil
    case DateTime => Nil
    case Interval => Nil
    case Set(value) => value :: Nil
    case AnonElem(value) => value :: Nil
    case IndexedElem(index, value) => value :: Nil
    case AnonField(value) => value :: Nil
    case NamedField(name, value) => value :: Nil
    case x : Product => x.flatten.toList
    case x : Coproduct => x.flatten.toList
  }

  def foldMap[Z: Monoid](f: Type => Z)(v: Type): Z = Monoid[Z].append(f(v), Foldable[List].foldMap(children(v))(foldMap(f)))

  def mapUp(v: Type)(f0: PartialFunction[Type, Type]): Type = {
    val f = f0.orElse[Type, Type] {
      case x => x
    }

    def loop(v: Type): Type = v match {
      case Top => f(v)
      case Bottom => f(v)
      case Const(value) =>
         val newType = f(value.dataType)

         if (newType != value.dataType) newType
         else f(newType)

      case Null => f(v)
      case Str => f(v)
      case Int => f(v)
      case Dec => f(v)
      case Bool => f(v)
      case Binary => f(v)
      case DateTime => f(v)
      case Interval => f(v)
      case Set(value) => f(loop(value))
      case AnonElem(value) => f(loop(value))
      case IndexedElem(index, value) => f(loop(value))
      case AnonField(value) => f(loop(value))
      case NamedField(name, value) => f(loop(value))
      case x : Product => f(Product(x.flatten.map(loop _)))
      case x : Coproduct => f(Coproduct(x.flatten.map(loop _)))
    }

    loop(v)
  }

  def mapUpM[F[_]: Monad](v: Type)(f: Type => F[Type]): F[Type] = {
    def loop(v: Type): F[Type] = v match {
      case Top => f(v)
      case Bottom => f(v)
      case Const(value) =>
         for {
          newType  <- f(value.dataType)
          newType2 <- if (newType != value.dataType) Monad[F].point(newType)
                      else f(newType)
        } yield newType2

      case Null     => f(v)
      case Str      => f(v)
      case Int      => f(v)
      case Dec      => f(v)
      case Bool     => f(v)
      case Binary   => f(v)
      case DateTime => f(v)
      case Interval => f(v)
      
      case Set(value)        => loop(value) >>= f
      case AnonElem(value)      => loop(value) >>= f
      case IndexedElem(_, value)  => loop(value) >>= f
      case AnonField(value)     => loop(value) >>= f
      case NamedField(_, value)   => loop(value) >>= f

      case x : Product => 
        for {
          xs <- Traverse[List].sequence(x.flatten.toList.map(loop _))
          v2 <- f(Product(xs))
        } yield v2

      case x : Coproduct =>
        for {
          xs <- Traverse[List].sequence(x.flatten.toList.map(loop _))
          v2 <- f(Product(xs))
        } yield v2
    }

    loop(v)
  }

  case object Top extends Type
  case object Bottom extends Type
  
  case class Const(value: Data) extends Type

  sealed trait PrimitiveType extends Type
  case object Null extends PrimitiveType
  case object Str extends PrimitiveType
  case object Int extends PrimitiveType
  case object Dec extends PrimitiveType
  case object Bool extends PrimitiveType
  case object Binary extends PrimitiveType
  case object DateTime extends PrimitiveType
  case object Interval extends PrimitiveType

  case class Set(value: Type) extends Type

  case class AnonElem(value: Type) extends Type
  case class IndexedElem(index: Int, value: Type) extends Type

  case class AnonField(value: Type) extends Type
  case class NamedField(name: String, value: Type) extends Type

  case class Product(left: Type, right: Type) extends Type {
    def flatten: Vector[Type] = {
      def flatten0(v: Type): Vector[Type] = v match {
        case Product(left, right) => flatten0(left) ++ flatten0(right)
        case x => Vector(x)
      }

      flatten0(this)
    }

    override def hashCode = flatten.toSet.hashCode()

    override def equals(that: Any) = that match {
      case that : Product => this.flatten.toSet.equals(that.flatten.toSet)

      case _ => false
    }
  }
  object Product extends ((Type, Type) => Type) {
    def apply(values: Seq[Type]): Type = {
      if (values.length == 0) Top
      else if (values.length == 1) values.head
      else values.tail.foldLeft[Type](values.head)(_ & _)
    }
  }

  case class Coproduct(left: Type, right: Type) extends Type {
    def flatten: Vector[Type] = {
      def flatten0(v: Type): Vector[Type] = v match {
        case Coproduct(left, right) => flatten0(left) ++ flatten0(right)
        case x => Vector(x)
      }

      flatten0(this)
    }

    override def hashCode = flatten.toSet.hashCode()

    override def equals(that: Any) = that match {
      case that : Coproduct => this.flatten.toSet.equals(that.flatten.toSet)

      case _ => false
    }
  }
  object Coproduct extends ((Type, Type) => Type) {
    def apply(values: Seq[Type]): Type = {
      if (values.length == 0) Bottom
      else if (values.length == 1) values.head
      else values.tail.foldLeft[Type](values.head)(_ | _)
    }
  }

  private def exists(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Unit] = actuals.headOption match {
    case Some(head) => typecheck(expected, head) ||| exists(expected, actuals.tail)
    case None => fail(expected, Product(actuals))
  }

  private def forall(expected: Type, actuals: Seq[Type]): ValidationNel[TypeError, Unit] = {
    actuals.headOption match {
      case Some(head) => typecheck(expected, head) +++ exists(expected, actuals.tail)
      case None => Validation.success(Top)
    }
  }

  private val typecheckPP = typecheck(_ +++ _, exists _)

  private val typecheckPC = typecheck(_ +++ _, forall _)

  private val typecheckCP = typecheck(_ ||| _, exists _)

  private val typecheckCC = typecheck(_ ||| _, forall _)

  private def typecheck(combine: (ValidationNel[TypeError, Unit], ValidationNel[TypeError, Unit]) => ValidationNel[TypeError, Unit], 
                        check: (Type, Seq[Type]) => ValidationNel[TypeError, Unit]) = (expecteds: Seq[Type], actuals: Seq[Type]) => {
    expecteds.foldLeft[ValidationNel[TypeError, Unit]](Validation.success(Unit)) {
      case (acc, expected) => {
        combine(acc, check(expected, actuals))
      }
    }
  }

  def makeObject(values: Iterable[(String, Type)]) = Product.apply(values.toList.map((NamedField.apply _).tupled))

  def makeArray(values: List[Type]): Type = {
    val consts = values.collect { case Const(data) => data }

    if (consts.length == values.length) Const(Data.Arr(consts))
    else Product(values.zipWithIndex.map(t => IndexedElem(t._2, t._1)))
  }

  val AnyArray = AnonElem(Top)

  val AnyObject = AnonField(Top)

  val AnySet = Set(Top)

  val Numeric = Int | Dec

  val Comparable = Numeric | Str | DateTime | Interval | Bool
}