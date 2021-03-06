/*
 * Copyright 2020 Precog Data
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

package quasar.impl.scheduler

import slamdata.Predef._

import quasar.{Condition, EffectfulQSpec}
import quasar.api.intentions.IntentionError, IntentionError._
import quasar.api.scheduler._, SchedulerError._
import quasar.connector._
import quasar.connector.scheduler._
import quasar.contrib.scalaz.MonadError_

import argonaut.Json
import argonaut.JsonCats._

import cats.Show
import cats.effect._
import cats.implicits._

import fs2.Stream

import java.util.UUID
import scala.concurrent.ExecutionContext

import SchedulerBuildersSpec._

final class SchedulerBuildersSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] {
  implicit val timer = IO.timer(ec)

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val ioCreateErrorME: MonadError_[IO, CreateError[Json]] =
    new MonadError_[IO, CreateError[Json]] {
      def raiseError[A](e: CreateError[Json]): IO[A] =
        IO.raiseError(new CreateErrorException(e))
      def handleError[A](fa: IO[A])(f: CreateError[Json] => IO[A]): IO[A] =
        fa.recoverWith {
          case CreateErrorException(e) => f(e)
        }
    }

  def builder(kind: SchedulerType, err: Option[InitializationError[Json]] = None) = new SchedulerBuilder[IO] {
    def schedulerType = kind
    def sanitizeConfig(inp: Json) = Json.jString("sanitized")
    def scheduler(config: Json)
        : Resource[IO, Either[InitializationError[Json], Scheduler[IO, Array[Byte], Json]]] =
      err match {
        case Some(a) =>
          a.asLeft[Scheduler[IO, Array[Byte], Json]].pure[Resource[IO, ?]]
        case None =>
          val scheduler = new Scheduler[IO, Array[Byte], Json]  {
            def entries: Stream[IO, (Array[Byte], Json)] =
              Stream.empty
            def addIntention(c: Json): IO[Either[IncorrectIntention[Json], Array[Byte]]] =
              IO(UUID.randomUUID.toString.getBytes.asRight[IncorrectIntention[Json]])
            def lookupIntention(i: Array[Byte]): IO[Either[IntentionNotFound[Array[Byte]], Json]] =
              IO(Json.jNull.asRight[IntentionNotFound[Array[Byte]]])
            def editIntention(i: Array[Byte], config: Json): IO[Condition[SchedulingError[Array[Byte], Json]]] =
              IO(Condition.normal[SchedulingError[Array[Byte], Json]]())
            def deleteIntention(i: Array[Byte]): IO[Condition[IntentionNotFound[Array[Byte]]]] =
              IO(Condition.normal[IntentionNotFound[Array[Byte]]]())
          }
          scheduler.asRight[InitializationError[Json]].pure[Resource[IO, ?]]
      }
  }

  def mkBuilders(lst: List[SchedulerBuilder[IO]]) = SchedulerBuilders[IO](lst)

  val fooKind = SchedulerType("foo", 1L)
  val barKind = SchedulerType("bar", 2L)

  "supported types" >> {
    "empty" >>* {
      for {
        builders <- mkBuilders(List())
        actual <- builders.supportedTypes
      } yield {
        actual must_=== Set()
      }
    }
    "non-empty" >>* {

      val expected = Set(fooKind)
      for {
        builders <- mkBuilders(List(builder(fooKind)))
        actual <- builders.supportedTypes
      } yield {
        actual must_=== expected
      }
    }
  }

  "sanitizeRef" >>* {
    val supported = SchedulerRef(fooKind, "supported", Json.jString("foo"))
    val unsupported = SchedulerRef(barKind, "unsupported", Json.jString("bar"))

    for {
      builders <- mkBuilders(List(builder(fooKind)))
      supportedSanitized <- builders.sanitizeRef(supported)
      unsupportedSanitized <- builders.sanitizeRef(unsupported)
    } yield {
      supportedSanitized must_=== supported.copy(config = Json.jString("sanitized"))
      unsupportedSanitized must_=== unsupported.copy(config = Json.jEmptyObject)
    }
  }

  "create" >> {
    "work for supported" >>* {
      val ref = SchedulerRef(fooKind, "supported", Json.jString(""))
      for {
        builders <- mkBuilders(List(builder(fooKind)))
        res <- builders.create(ref).value.use { res => IO(res must beRight) }
      } yield res
    }
    "doesn't work for unsupported" >>* {
      val ref = SchedulerRef(barKind, "unsupported", Json.jString(""))
      for {
        builders <- mkBuilders(List(builder(fooKind)))
        res <- builders.create(ref).value.use { res => IO(res must beLeft(SchedulerUnsupported(barKind, Set(fooKind)))) }
      } yield res
    }
  }

  "enable/disable" >>* {
    val ref = SchedulerRef(fooKind, "supported", Json.jString(""))
    for {
      builders <- mkBuilders(List())
      (m0, finish0) <- builders.create(ref).value.allocated
      _ <- builders.enable(builder(fooKind))
      (m1, finish1) <- builders.create(ref).value.allocated
      _ <- builders.disable(fooKind)
      (m2, finish2) <- builders.create(ref).value.allocated
      _ <- finish0
      _ <- finish1
      _ <- finish2
    } yield {
      m0 must beLeft(SchedulerUnsupported(fooKind, Set()))
      m1 must beRight
      m2 must beLeft(SchedulerUnsupported(fooKind, Set()))
    }
  }

  "errors with initialization error" >>* {
    val malformed = SchedulerType("malformed", 1L)
    val invalid = SchedulerType("invalid", 1L)
    val connFailed = SchedulerType("conn-failed", 1L)
    val accessDenied = SchedulerType("access-denied", 1L)

    val malformedRef = SchedulerRef(malformed, "doesn't matter", Json.jString("malformed"))
    val connFailedRef = SchedulerRef(connFailed, "doesn't matter", Json.jString("connection failed"))
    val invalidRef = SchedulerRef(invalid, "doesn't matter", Json.jString("invalid"))
    val accessDeniedRef = SchedulerRef(accessDenied, "doesn't matter", Json.jString("access denied"))

    val malformedErr = MalformedConfiguration(malformed, Json.jString("a"), "malformed")
    val invalidErr = InvalidConfiguration(invalid, Json.jString("b"), Set("invalid"))
    val connErr = ConnectionFailed(connFailed, Json.jString("c"), new Exception("conn"))
    val accessErr = AccessDenied(accessDenied, Json.jString("d"), "access denied")

    val ioBuilders = mkBuilders(List(
      builder(malformed, Some(malformedErr)),
      builder(invalid, Some(invalidErr)),
      builder(connFailed, Some(connErr)),
      builder(accessDenied, Some(accessErr))))

    for {
      builders <- ioBuilders
      (malformedS, _) <- builders.create(malformedRef).value.allocated
      (invalidS, _) <- builders.create(invalidRef).value.allocated
      (connFailedS, _) <- builders.create(connFailedRef).value.allocated
      (accessDeniedS, _) <- builders.create(accessDeniedRef).value.allocated
    } yield {
      malformedS must beLeft(malformedErr)
      invalidS must beLeft(invalidErr)
      connFailedS must beLeft(connErr)
      accessDeniedS must beLeft(accessErr)
    }
  }
}

object SchedulerBuildersSpec {
  final case class CreateErrorException(ce: CreateError[Json])
    extends Exception(Show[SchedulerError[Int, Json]].show(ce))
}
