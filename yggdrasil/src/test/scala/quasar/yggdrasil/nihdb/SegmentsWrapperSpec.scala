/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil
package nihdb

import quasar.contrib.cats.effect._
import quasar.niflheim.{Chef, NIHDB, V1CookedBlockFormat, V1SegmentFormat, VersionedCookedBlockFormat, VersionedSegmentFormat}
import quasar.precog.common._
import quasar.precog.util.IOUtils
import quasar.yggdrasil.table._

import akka.actor.{ActorRef, ActorSystem, Props}

import cats.effect.IO

import org.scalacheck._
import org.specs2.ScalaCheck
import org.specs2.mutable._

import scalaz.std.list._
import scalaz.syntax.traverse._

import shims._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import java.nio.file.Files
import java.util.concurrent.{ScheduledThreadPoolExecutor, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger

object SegmentsWrapperSpec extends Specification with ScalaCheck {
  import ArbitrarySlice._

  // cargo-culted all this setup, mostly from Precog
  val CookThreshold = 1
  val Timeout = 300.seconds

  private val TxLogScheduler = new ScheduledThreadPoolExecutor(20,
    new ThreadFactory {
      private val counter = new AtomicInteger(0)

      def newThread(r: Runnable): Thread = {
        val t = new Thread(r)
        t.setName("HOWL-sched-%03d".format(counter.getAndIncrement()))

        t
      }
    })

  implicit val actorSystem = ActorSystem(
    "nihdbExecutorActorSystem",
    classLoader = Some(getClass.getClassLoader))

  private val props: Props = Props(Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat))))

  val masterChef: ActorRef = actorSystem.actorOf(props)

  "direct columnmar table persistence" should {
    val paths = Vector(
      CPath("0") -> CLong,
      CPath("1") -> CBoolean,
      CPath("2") -> CString,
      CPath("3") -> CDouble,
      CPath("4") -> CNum,
      CPath("5") -> CEmptyObject,
      CPath("6") -> CEmptyArray,
      CPath("7") -> CNum)

    val pd = paths.toList map {
      case (cpath, ctype) =>
        ColumnRef(cpath, ctype)
    }

    implicit def arbSlice = Arbitrary(for {
      badSize <- Arbitrary.arbitrary[Int]
      size = scala.math.abs(badSize % 100).toInt
      slice <- genSlice(pd, size)
    } yield slice)

    "persist arbitrary tables" in prop { slices: List[Slice] =>
      val dir = Files.createTempDirectory("SegmentsWrapperSpec").toFile
      try {
        val ioa = for {
          nihdbV <- IO {
            NIHDB.create(masterChef, dir, CookThreshold, Timeout, TxLogScheduler).unsafePerformIO
          }

          _ = nihdbV.toEither must beRight
          nihdb = nihdbV.toEither.right.get

          _ <- slices.zipWithIndex traverse {
            case (slice, index) =>
              val segments = SegmentsWrapper.sliceToSegments(index.toLong, slice)

              IO.fromFutureShift(
                IO(nihdb.insertSegmentsVerified(index.toLong, slice.size, segments)))
          }

          proj <- NIHDBProjection.wrap(nihdb)
          stream <- proj.getBlockStream(None).toStream
        } yield {
          val slices2 = stream.toList.map(_.deref("value"))

          slices.flatMap(_.toRValues) mustEqual slices2.flatMap(_.toRValues)
        }

        ioa.unsafeRunSync
      } finally {
        IOUtils.recursiveDelete(dir).unsafePerformIO
      }
    }.set(
      minTestsOk = Runtime.getRuntime.availableProcessors * 2,
      workers = Runtime.getRuntime.availableProcessors)    // these take a long time
  }

  protected def afterAll() = {
    actorSystem.terminate
  }
}
