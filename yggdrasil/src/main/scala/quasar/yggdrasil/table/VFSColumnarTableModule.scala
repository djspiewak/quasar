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

package quasar.yggdrasil.table

import quasar.yggdrasil._
import quasar.yggdrasil.bytecode.JType
import quasar.precog.common.security._
import quasar.yggdrasil.vfs._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global // FIXME what is this thing

import akka.pattern.AskSupport

import scalaz._
import scalaz.std.list._
import scalaz.syntax.traverse._

import org.slf4s.Logging

trait VFSColumnarTableModule extends BlockStoreColumnarTableModule[Future] with Logging {

  trait VFSColumnarTableCompanion extends BlockStoreColumnarTableCompanion {
    def load(table: Table, apiKey: APIKey, tpe: JType): EitherT[Future, ResourceError, Table] = ???   // TODO
  }
}