/*
 * Copyright 2014–2019 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.impl.push

import slamdata.Predef._

import argonaut._, Argonaut._

import cats.data.{NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.Eq
import cats.effect.IO
import cats.implicits._

import eu.timepit.refined.auto._

import fs2.concurrent.SignallingRef
import fs2.job.JobManager
import fs2.{Stream, text}

import org.specs2.matcher.MatchResult

import quasar.{ConditionMatchers, EffectfulQSpec}
import quasar.api.QueryEvaluator
import quasar.api.destination.{Destination, DestinationType, Label, Labeled, ResultSink, ResultType, TypeCoercion, UntypedDestination}
import quasar.api.destination.param.Param
import quasar.api.push.{PushMeta, RenderConfig, ResultPush, ResultPushError, ResultRender, Status}
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.api.table.{ColumnType, TableColumn, TableName, TableRef}
import quasar.fp.Dependent

import shims.{eqToScalaz, showToScalaz, showToCats}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  implicit val tmr = IO.timer(global)

  val TableId = 42
  val DestinationId = 43
  val RefDestinationType = DestinationType("ref", 1L)

  // convert a bytestream into stream of space separeted words
  def bytesToString(s: Stream[IO, Byte]): Stream[IO, String] =
    s.split(_ == 0x20).flatMap(Stream.chunk).through(text.utf8Decode)

  type Filesystem = Map[ResourcePath, String]

  final class RefDestination(ref: SignallingRef[IO, Filesystem]) extends UntypedDestination[IO] {
    def destinationType: DestinationType = RefDestinationType
    def sinks = NonEmptyList.of(csvSink)

    val csvSink: ResultSink[IO, Unit] = ResultSink.csv[IO, Unit](RenderConfig.Csv()) {
      case (dst, columns, bytes) =>
        bytesToString(bytes).evalMap(str =>
          ref.update(currentFs =>
            currentFs + (dst -> currentFs.get(dst).fold(str)(_ ++ str))))
    }
  }

  object RefDestination {
    def apply(): IO[(Destination[IO], SignallingRef[IO, Filesystem])] =
      for {
        fs <- SignallingRef[IO, Filesystem](Map.empty)
        destination = new RefDestination(fs)
      } yield (destination, fs)
  }

  final class RefTypedDestination(ref: SignallingRef[IO, Filesystem]) extends Destination[IO] {
    sealed trait Type extends Product with Serializable

    object Type {
      final case class Varchar(n: Int) extends Type
      case object Integer extends Type
    }

    sealed trait Constructor[A] extends ConstructorLike[A] with Product with Serializable

    object Constructor {
      case object VarcharC extends Constructor[Int] {
        val param = Param.Integer(Some(0), Some(255), None)
        def apply(n: Int) = Type.Varchar(n)
      }
    }

    implicit val labelType: Label[Type] =
      Label label {
        case Type.Varchar(n) => s"Varchar($n)"
        case Type.Integer => s"Integer"
      }

    implicit val eqType: Eq[Type] = Eq.fromUniversalEquals[Type]

    implicit val jsonCodecType: CodecJson[Type] =
      CodecJson[Type](
        {
          case Type.Varchar(n) =>
            Json("varchar" -> n.asJson)

          case Type.Integer =>
            "integer".asJson
        },
        { cursor =>
          val direct = cursor.as[String] flatMap {
            case "integer" => DecodeResult.ok[Type](Type.Integer)
            case t => DecodeResult.fail[Type](s"unknown type tag $t", cursor.history)
          }

          val varchar = (cursor --\ "varchar").as[Int].map(Type.Varchar(_): Type)

          direct ||| varchar
        })

    implicit def labelConstructor[P]: Label[Constructor[P]] =
      Label label {
        case Constructor.VarcharC => "Varchar"
      }

    implicit def eqConstructor[P]: Eq[Constructor[P]] =
      Eq.fromUniversalEquals[Constructor[P]]

    implicit def jsonCodecConstructor[P]: CodecJson[Constructor[P]] =
      CodecJson[Constructor[P]](
        {
          case Constructor.VarcharC => "varchar".asJson
        },
        { c =>
          c.as[String] flatMap {
            case "varchar" => DecodeResult.ok[Constructor[P]](Constructor.VarcharC: Constructor[P])
            case tag => DecodeResult.fail[Constructor[P]](s"unknown constructor tag '$tag'", c.history)
          }
        })

    DecodeJson[Exists[Constructor]] { c =>
      c.as[String] flatMap {
        case "varchar" =>
          DecodeResult.ok[Exists[Constructor]] {
            new Exists[Constructor] {
              val P = Int
              val c = Constructor.VarcharC
            }
          }
        case tag => DecodeResult.fail[Exists[Constructorjh]](s"unknown constructor tag '$tag'", c.history)
      }
    }

    trait Exists[C[_]] {
      type P
      val c: C[P]

      def skolemize: C[τ] = c
    }

    type ∃[C[_]] = Exists[C]
    val ∃ = Exists

    object Exists {
      def apply[C[_]]: PartiallyApplied[C] =
        new PartiallyApplied[C]

      final class PartiallyApplied[C] {
        def apply[A](ca: C[A]): Exists[C] =
          new Exists {
            type P = A
            val c = ca
          }
      }
    }

    DecodeJson[ConstructorContainer]

    implicit val dependentLabel: Dependent[Constructor, Label] =
      λ[Dependent[Constructor, Label]] {
        case Constructor.VarcharC =>
          Label.label[Int](_ => "Integer")
      }

    implicit val dependentEq: Dependent[Constructor, Eq] =
      λ[Dependent[Constructor, Eq]] {
        case Constructor.VarcharC => Eq[Int]
      }

    implicit val dependentCodecJson: Dependent[Constructor, CodecJson] =
      λ[Dependent[Constructor, CodecJson]] {
        case Constructor.VarcharC =>
          CodecJson[Int](
            _.asJson,
            _.as[Int])
      }

    final def coerce(tpe: ColumnType): TypeCoercion[Constructor, Type] = tpe match {
      case ColumnType.Number =>
        TypeCoercion.Satisfied(NonEmptyList.one(Right(Type.Integer)))

      case ColumnType.String =>
        TypeCoercion.Satisfied[Constructor, Type](
          NonEmptyList.one(
            Labeled(
              "Varchar",
              TypeCoercion.Unapplied(Constructor.VarcharC, Constructor.VarcharC.param)).asLeft[Type]))

      case _ =>
        TypeCoercion.Unsatisfied(Nil, None)
    }

    def destinationType: DestinationType = RefDestinationType
    def sinks = NonEmptyList.of(csvSink)

    val csvSink: ResultSink[IO, Type] = ResultSink.csv[IO, Type](RenderConfig.Csv()) {
      case (dst, columns, bytes) =>
        bytesToString(bytes).evalMap(str =>
          ref.update(currentFs =>
            currentFs + (dst -> currentFs.get(dst).fold(str)(_ ++ str))))
    }
  }

  object RefTypedDestination {
    def apply(): IO[(Destination[IO], SignallingRef[IO, Filesystem])] =
      for {
        fs <- SignallingRef[IO, Filesystem](Map.empty)
        destination = new RefTypedDestination(fs)
      } yield (destination, fs)
  }

  final class MockResultRender extends ResultRender[IO, String] {
    def renderCsv(
        input: String,
        columns: List[TableColumn],
        config: RenderConfig.Csv,
        limit: Option[Long])
        : Stream[IO, Byte] =
      Stream(input).through(text.utf8Encode)

    def renderJson(
        input: String,
        prefix: String,
        delimiter: String,
        suffix: String)
        : Stream[IO, Byte] =
      Stream.empty
  }

  def mkEvaluator(fn: String => IO[Stream[IO, String]]): QueryEvaluator[IO, String, Stream[IO, String]] =
    new QueryEvaluator[IO, String, Stream[IO, String]] {
      def evaluate(query: String): IO[Stream[IO, String]] =
        fn(query)
    }

  def mkResultPush(
    tables: Map[Int, TableRef[String]],
    destinations: Map[Int, Destination[IO]],
    manager: JobManager[IO, (Int, Int), Nothing],
    evaluator: QueryEvaluator[IO, String, Stream[IO, String]])
      : IO[ResultPush[IO, Int, Int, Json]] = {

    val lookupTable: Int => IO[Option[TableRef[String]]] =
      tableId => IO(tables.get(tableId))

    val lookupDestination: Int => IO[Option[Destination[IO]]] =
      destinationId => IO(destinations.get(destinationId))

    val render = new MockResultRender

    DefaultResultPush[IO, Int, Int, String, String](
      lookupTable,
      evaluator,
      lookupDestination,
      manager,
      render)
  }

  val jobManager =
    JobManager[IO, (Int, Int), Nothing]()
      .compile
      .resource
      .lastOrError

  def mockEvaluate(q: String): String =
    s"evaluated($q)"

  val WorkTime = Duration(400, MILLISECONDS)
  val Timeout = 5 * WorkTime

  val await = IO.sleep(WorkTime * 2)
  val awaitS = Stream.sleep_(WorkTime)

  def latchGet(s: SignallingRef[IO, String], expected: String): IO[Unit] =
    s.discrete.filter(_ === expected).take(1).compile.drain
      .timeoutTo(Timeout * 2, IO.raiseError(new Exception("latchGet timed out")))

  def waitForUpdate[K, V](s: SignallingRef[IO, Map[K, V]]): IO[Unit] =
    s.discrete.filter(_.nonEmpty).take(1).compile.drain
      .timeoutTo(Timeout, IO.raiseError(new Exception("waitForUpdate timed out")))

  /* Runs `io` with a timeout, producing a failing expectation if `io`
     completes before the timeout. errMsg is included with the failing expectation. */
  def verifyTimeout[A](io: IO[A], errMsg: String): IO[MatchResult[Any]] =
    (io >> IO(ko(errMsg))).timeoutTo(Timeout, IO(ok))


  "start" >> {
    "asynchronously pushes a table to a destination" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"
      val testTable = TableRef(TableName("foo"), query, List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          ref <- SignallingRef[IO, String]("Not started")
          evaluator = mkEvaluator(q => IO(Stream.emit(mockEvaluate(q)) ++ Stream.eval_(ref.set("Finished"))))

          push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, evaluator)
          startRes <- push.start(TableId, Nil, DestinationId, pushPath, ResultType.Csv, None)
          _ <- latchGet(ref, "Finished")
          filesystemAfterPush <- waitForUpdate(filesystem) >> filesystem.get
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPath))
          filesystemAfterPush(pushPath) must equal(mockEvaluate(query))
          startRes must beNormal
        }
      }
    }

    "rejects an already running push of a table to the same destination" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          sync <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream.eval(sync.set("Started")).as("1") ++ awaitS.as("2") ++ Stream.eval(sync.set("Finished")).as("3"))))
          firstStartStatus <- push.start(TableId, Nil, DestinationId, path, ResultType.Csv, None)
          secondStartStatus <- push.start(TableId, Nil, DestinationId, path, ResultType.Csv, None)
        } yield {
          firstStartStatus must beNormal
          secondStartStatus must beAbnormal(ResultPushError.PushAlreadyRunning(TableId, DestinationId))
        }
      }
    }

    "allows concurrent push of a table to multiple destinations" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())
      val dest2 = DestinationId + 1

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          sync <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination, dest2 -> destination),
            jm,
            mkEvaluator(_ => IO(Stream.eval(sync.set("Started")).as("1") ++ awaitS.as("2") ++ Stream.eval(sync.set("Finished")).as("3"))))
          firstStartStatus <- push.start(TableId, Nil, DestinationId, path, ResultType.Csv, None)
          secondStartStatus <- push.start(TableId, Nil, dest2, path, ResultType.Csv, None)
        } yield {
          firstStartStatus must beNormal
          secondStartStatus must beNormal
        }
      }
    }

    "fails with 'destination not found' when destination doesn't exist" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream("1", "2", "3"))))
          startStatus <- push.start(TableId, Nil, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beAbnormal(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }

    "fails with 'table not found' when table doesn't exist" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (dest, _) <- RefDestination()
          push <- mkResultPush(
            Map(),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream("1", "2", "3"))))
          startStatus <- push.start(TableId, Nil, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beAbnormal(ResultPushError.TableNotFound(TableId))
        }
      }
    }

    "fails when destination doesn't support requested format" >> todo
  }

  "start these" >> {
    val path1 = ResourcePath.root() / ResourceName("foo")
    val testTable1 = TableRef(TableName("foo"), "queryFoo", List())

    val path2 = ResourcePath.root() / ResourceName("bar")
    val testTable2 = TableRef(TableName("bar"), "queryBar", List())

    "starts a push for each table to the destination" >>* {
      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          ref1 <- SignallingRef[IO, String]("Not started")
          ref2 <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(1 -> testTable1, 2 -> testTable2),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case "queryFoo" =>
                IO(Stream.eval_(ref1.set("Started")) ++ awaitS ++ Stream.eval_(ref1.set("Finished")))
              case "queryBar" =>
                IO(Stream.eval_(ref2.set("Started")) ++ awaitS ++ Stream.eval_(ref2.set("Finished")))
            })

          errors <- push.startThese(DestinationId, NonEmptyMap.of(
            1 -> ((Nil, path1, ResultType.Csv, None)),
            2 -> ((Nil, path2, ResultType.Csv, None))))

          _ <- latchGet(ref1, "Finished") >> latchGet(ref2, "Finished")
        } yield {
          errors.isEmpty must beTrue
        }
      }
    }

    "returns the cause of any pushes that failed to start" >>* {
      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          ref2 <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(2 -> testTable2),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case "queryBar" =>
                IO(Stream.eval_(ref2.set("Started")) ++ awaitS ++ Stream.eval_(ref2.set("Finished")))
            })

          errors <- push.startThese(DestinationId, NonEmptyMap.of(
            1 -> ((Nil, path1, ResultType.Csv, None)),
            2 -> ((Nil, path2, ResultType.Csv, None))))

          _ <- latchGet(ref2, "Finished")
        } yield {
          errors must_=== Map(1 -> ResultPushError.TableNotFound(1))
        }
      }
    }
  }

  "cancel" >> {
    "aborts a running push" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"
      val testTable = TableRef(TableName("foo"), query, List())

      def testStream(ref: SignallingRef[IO, String]): Stream[IO, String] =
        Stream("foo") ++
          Stream(" ") ++ // chunk delimiter
          Stream.eval_(ref.set("Working")) ++
          Stream.sleep_(WorkTime) ++
          Stream("bar") ++
          Stream.eval_(ref.set("Finished"))

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          ref <- SignallingRef[IO, String]("Not started")

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(testStream(ref))))

          startRes <- push.start(TableId, Nil, DestinationId, pushPath, ResultType.Csv, None)
          _ <- latchGet(ref, "Working")
          cancelRes <- push.cancel(TableId, DestinationId)

          filesystemAfterPush <- waitForUpdate(filesystem) >> filesystem.get
          // fail the test if push evaluation was not cancelled
          evaluationFinished <- verifyTimeout(latchGet(ref, "Finished"), "Push not cancelled")
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPath))
          // check if a *partial* result was pushed
          filesystemAfterPush(pushPath) must equal("foo")
          startRes must beNormal
          cancelRes must beNormal
          evaluationFinished
        }
      }
    }

    "no-op when push already completed" >>* {
      val data = Stream("1", "2", "3")
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          ref <- SignallingRef[IO, String]("Not started")

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data ++ Stream.eval_(ref.set("Finished")))))

          startRes <- push.start(TableId, Nil, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- latchGet(ref, "Finished")
          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          startRes must beNormal
          cancelRes must beNormal
        }
      }
    }

    "no-op when no push for extant table" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, _) <- RefDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beNormal
        }
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beAbnormal(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }

    "returns 'table not found' when table doesn't exist" >>* {
      jobManager use { jm =>
        for {
          (dest, _) <- RefDestination()

          push <- mkResultPush(
            Map(),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beAbnormal(ResultPushError.TableNotFound(TableId))
        }
      }
    }
  }

  "cancel these" >> {
    "cancels running pushes" >>* {
      val queryA = "queryA"
      val queryB = "queryB"
      val queryC = "queryC"

      val testTableA = TableRef(TableName("A"), queryA, List())
      val testTableB = TableRef(TableName("B"), queryB, List())
      val testTableC = TableRef(TableName("C"), queryC, List())

      val pushPathA = ResourcePath.root() / ResourceName("foo") / ResourceName("A")
      val pushPathB = ResourcePath.root() / ResourceName("foo") / ResourceName("B")

      val idA = TableId
      val idB = idA + 1
      val idC = idB + 1

      def testStream(ref: SignallingRef[IO, String]): Stream[IO, String] =
        Stream("foo") ++
          Stream(" ") ++ // chunk delimiter
          Stream.eval_(ref.set("Working")) ++
          Stream.sleep_(WorkTime) ++
          Stream("bar") ++
          Stream.eval_(ref.set("Finished"))

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()

          refA <- SignallingRef[IO, String]("Not started")
          refB <- SignallingRef[IO, String]("Not started")

          push <- mkResultPush(
            Map(
              idA -> testTableA,
              idB -> testTableB,
              idC -> testTableC),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case `queryA` => IO(testStream(refA))
              case `queryB` => IO(testStream(refB))
            })

          startRes <- push.startThese(
            DestinationId,
            NonEmptyMap.of(
              idA -> ((Nil, pushPathA, ResultType.Csv, None)),
              idB -> ((Nil, pushPathB, ResultType.Csv, None))))

          _ <- latchGet(refA, "Working")
          _ <- latchGet(refB, "Working")

          cancelRes <- push.cancelThese(
            DestinationId,
            NonEmptySet.of(idA, idB, idC))

          filesystemAfterPush <- waitForUpdate(filesystem) >> filesystem.get
          // fail the test if push evaluation was not cancelled
          evaluationAFinished <- verifyTimeout(latchGet(refA, "Finished"), "Push A not cancelled")
          evaluationBFinished <- verifyTimeout(latchGet(refB, "Finished"), "Push B not cancelled")
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPathA, pushPathB))
          // check if a *partial* result was pushed
          filesystemAfterPush(pushPathA) must equal("foo")
          filesystemAfterPush(pushPathB) must equal("foo")
          startRes.isEmpty must beTrue
          cancelRes.isEmpty must beTrue
          evaluationAFinished and evaluationBFinished
        }
      }
    }

    "returns the cause of any push that failed to cancel" >>* {
      val queryA = "queryA"
      val queryB = "queryB"

      val testTableA = TableRef(TableName("A"), queryA, List())
      val testTableB = TableRef(TableName("B"), queryB, List())

      val pushPathA = ResourcePath.root() / ResourceName("foo") / ResourceName("A")

      val idA = TableId
      val idB = idA + 1

      def testStream(ref: SignallingRef[IO, String]): Stream[IO, String] =
        Stream("foo") ++
          Stream(" ") ++ // chunk delimiter
          Stream.eval_(ref.set("Working")) ++
          Stream.sleep_(WorkTime) ++
          Stream("bar") ++
          Stream.eval_(ref.set("Finished"))

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()

          refA <- SignallingRef[IO, String]("Not started")

          push <- mkResultPush(
            Map(idA -> testTableA),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case `queryA` => IO(testStream(refA))
            })

          startRes <- push.start(
            idA,
            Nil,
            DestinationId,
            pushPathA,
            ResultType.Csv,
            None)

          _ <- latchGet(refA, "Working")

          cancelRes <- push.cancelThese(DestinationId, NonEmptySet.of(idA, idB))

          filesystemAfterPush <- waitForUpdate(filesystem) >> filesystem.get
          // fail the test if push evaluation was not cancelled
          evaluationAFinished <- verifyTimeout(latchGet(refA, "Finished"), "Push A not cancelled")
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPathA))
          // check if a *partial* result was pushed
          filesystemAfterPush(pushPathA) must equal("foo")
          startRes must beNormal
          cancelRes must_=== Map(idB -> ResultPushError.TableNotFound(idB))
          evaluationAFinished
        }
      }
    }
  }

  "destination status" >> {
    "empty when nothing has been pushed" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream())))
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus must beLike {
            case Right(m) => m.isEmpty must beTrue
          }
        }
      }
    }

    "includes running push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          sync <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream.eval(sync.set("Started")).as("1") ++ awaitS ++ Stream.eval(sync.set("Finished")).as("2"))))
          _ <- push.start(TableId, Nil, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- latchGet(sync, "Started")
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus.map(_.get(TableId)) must beLike {
            case Right(Some(PushMeta(_, ResultType.Csv, None, Status.Running(_)))) => ok
          }
        }
      }
    }

    "includes canceled push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          sync <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream.eval(sync.set("Started")).as("1") ++ awaitS ++ Stream.eval(sync.set("Finished")).as("2"))))
          _ <- push.start(TableId, Nil, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- latchGet(sync, "Started")
          _ <- push.cancel(TableId, DestinationId)
          _ <- await
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus.map(_.get(TableId)) must beLike {
            case Right(Some(PushMeta(_, ResultType.Csv, None, Status.Canceled(_, _)))) => ok
          }
        }
      }
    }

    "includes completed push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          sync <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream.eval_(sync.set("Started")) ++ awaitS ++ Stream.eval_(sync.set("Finished")))))
          _ <- push.start(TableId, Nil, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- latchGet(sync, "Finished")
          _ <- await // wait for concurrent update of push status
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus.map(_.get(TableId)) must beLike {
            case Right(Some(PushMeta(_, ResultType.Csv, None, Status.Finished(_, _)))) => ok
          }
        }
      }
    }

    "includes pushes that failed to initialize along with error that led to failure" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO.raiseError[Stream[IO, String]](new Exception("boom"))))
          pushStatus <- push.start(TableId, Nil, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- await // wait for error handling
          status <- push.destinationStatus(DestinationId)
        } yield {
          status.map(_.get(TableId)) must beLike {
            case Right(Some(PushMeta(_, ResultType.Csv, None, Status.Failed(ex, _, _)))) =>
              ex.getMessage must equal("boom")
          }
        }
      }
    }

    "includes pushes that failed during streaming along with the error that led to falure" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())
      val ex = new Exception("boom")

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          sync <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream.eval_(sync.set("Started")) ++ Stream.raiseError[IO](ex))))
          _ <- push.start(TableId, Nil, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- latchGet(sync, "Started")
          _ <- await // wait for error handling
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus.map(_.get(TableId)) must beLike {
            case Right(Some(PushMeta(_, ResultType.Csv, None, Status.Failed(ex, _, _)))) =>
              ex.getMessage must equal("boom")
          }
        }
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus must beLeft(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }
  }

  "cancel all" >> {
    "aborts all running pushes" >>* {
      val path1 = ResourcePath.root() / ResourceName("foo")
      val testTable1 = TableRef(TableName("foo"), "queryFoo", List())

      val path2 = ResourcePath.root() / ResourceName("bar")
      val testTable2 = TableRef(TableName("bar"), "queryBar", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- RefDestination()
          refFoo <- SignallingRef[IO, String]("Not started")
          refBar <- SignallingRef[IO, String]("Not started")
          push <- mkResultPush(
            Map(1 -> testTable1, 2 -> testTable2),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case "queryFoo" =>
                IO(Stream.eval_(refFoo.set("Started")) ++ awaitS ++ Stream.eval_(refFoo.set("Finished")))
              case "queryBar" =>
                IO(Stream.eval_(refBar.set("Started")) ++ awaitS ++ Stream.eval_(refBar.set("Finished")))
            })
          _ <- push.start(1, Nil, DestinationId, path1, ResultType.Csv, None)
          _ <- push.start(2, Nil, DestinationId, path2, ResultType.Csv, None)
          _ <- latchGet(refFoo, "Started") >> latchGet(refBar, "Started")
          _ <- push.cancelAll
          fooCanceled <- verifyTimeout(latchGet(refFoo, "Finished"), "queryFoo not cancelled")
          barCanceled <- verifyTimeout(latchGet(refBar, "Finished"), "queryBar not cancelled")
        } yield ok
      }
    }

    "no-op when no running pushes" >>* {
      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))
          _ <- push.cancelAll
        } yield ok
      }
    }
  }
}
