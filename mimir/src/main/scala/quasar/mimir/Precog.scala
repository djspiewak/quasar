/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.mimir

import quasar.blueeyes.json.JValue
import quasar.blueeyes.util.Clock
import quasar.niflheim.{Chef, V1CookedBlockFormat, V1SegmentFormat, VersionedSegmentFormat, VersionedCookedBlockFormat}
import quasar.precog.common.Path
import quasar.precog.common.accounts.AccountFinder

import quasar.precog.common.security.{
  APIKey,
  APIKeyFinder,
  APIKeyManager,
  DirectAPIKeyFinder,
  InMemoryAPIKeyManager,
  PermissionsFinder
}

import quasar.yggdrasil.PathMetadata
import quasar.yggdrasil.table.VFSColumnarTableModule
import quasar.yggdrasil.vfs.ResourceError

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.routing.{
  ActorRefRoutee,
  CustomRouterConfig,
  RouterConfig,
  RoundRobinRoutingLogic,
  Routee,
  Router
}

import scalaz.{EitherT, Monad, StreamT}
import scalaz.std.scalaFuture.futureInstance

import java.io.File
import java.time.Instant

import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.collection.immutable.IndexedSeq

// calling this constructor is a side-effect; you must always shutdown allocated instances
class Precog(dataDir0: File) extends VFSColumnarTableModule {

  object Config {
    val howManyChefsInTheKitchen: Int = 4
    val cookThreshold: Int = 20000
    val storageTimeout: FiniteDuration = new FiniteDuration(300, SECONDS)
    val quiescenceTimeout: FiniteDuration = new FiniteDuration(300, SECONDS)
    val maxOpenPaths: Int = 500
    val dataDir: File = dataDir0
  }

  // for the time being, do everything with this key
  def RootAPIKey: Future[APIKey] = emptyAPIKeyManager.rootAPIKey

  // Members declared in quasar.yggdrasil.vfs.ActorVFSModule
  private lazy val emptyAPIKeyManager: APIKeyManager[Future] =
    new InMemoryAPIKeyManager[Future](Clock.System)

  private val apiKeyFinder: APIKeyFinder[Future] =
    new DirectAPIKeyFinder[Future](emptyAPIKeyManager)

  private val accountFinder: AccountFinder[Future] = AccountFinder.Singleton(RootAPIKey)

  def permissionsFinder: PermissionsFinder[Future] =
    new PermissionsFinder(apiKeyFinder, accountFinder, Instant.EPOCH)

  private val actorSystem: ActorSystem =
    ActorSystem("nihdbExecutorActorSystem")

  private val props: Props = Props(Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat))))

  private def chefs(system: ActorSystem): IndexedSeq[Routee] =
    (1 to Config.howManyChefsInTheKitchen).map { _ =>
      ActorRefRoutee(system.actorOf(props))
    }

  private val routerConfig: RouterConfig = new CustomRouterConfig {
    def createRouter(system: ActorSystem): Router =
      Router(RoundRobinRoutingLogic(), chefs(system))
  }

  // needed for nihdb
  private val masterChef: ActorRef =
    actorSystem.actorOf(props.withRouter(routerConfig))

  private val clock: Clock = Clock.System

  // Members declared in quasar.yggdrasil.table.ColumnarTableModule
  implicit def M: Monad[Future] = futureInstance

  // Members declared in quasar.yggdrasil.TableModule
  sealed trait TableCompanion extends VFSColumnarTableCompanion
  object Table extends TableCompanion

  def showContents(path: Path): EitherT[Future, ResourceError, Set[PathMetadata]] = ???   // TODO

  def stopPath(path: Path): Unit = ???

  // TODO this could be trivially rewritten with fs2.Stream
  def ingest(path: Path, chunks: StreamT[Future, Vector[JValue]]): Future[Unit] = ???   // TODO

  def shutdown: Future[Unit] = actorSystem.terminate.map(_ => ())
}