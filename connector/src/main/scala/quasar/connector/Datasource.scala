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

package quasar.connector

import slamdata.Predef.{Boolean, Option}
import quasar.api.QueryEvaluator
import quasar.api.datasource.DatasourceType
import quasar.api.resource._

import monocle.{Lens, PLens}

/** @tparam F effects
  * @tparam G multiple results
  * @tparam Q query
  */
trait Datasource[F[_], G[_], Q, R] extends QueryEvaluator[F, Q, R] {

  type PathType <: ResourcePathType

  /** The type of this datasource. */
  def kind: DatasourceType

  /** Returns whether or not the specified path refers to a resource in the
    * specified datasource.
    */
  def pathIsResource(path: ResourcePath): F[Boolean]

  /** Returns the name and type of the `ResourcePath`s implied by concatenating
    * each name to `prefixPath` or `None` if `prefixPath` does not exist.
    */
  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, PathType)]]]
}

object Datasource {
  type Aux[F[_], G[_], Q, R, P <: ResourcePathType] = Datasource[F, G, Q, R] { type PathType = P }

  def evaluator[F[_], G[_], Q, R, P <: ResourcePathType]: Lens[Datasource.Aux[F, G, Q, R, P], QueryEvaluator[F, Q, R]] =
    pevaluator[F, G, Q, R, Q, R, P]

  def pevaluator[F[_], G[_], Q1, R1, Q2, R2, P <: ResourcePathType]
      : PLens[Datasource.Aux[F, G, Q1, R1, P], Datasource.Aux[F, G, Q2, R2, P], QueryEvaluator[F, Q1, R1], QueryEvaluator[F, Q2, R2]] =
    PLens((ds: Datasource.Aux[F, G, Q1, R1, P]) => ds: QueryEvaluator[F, Q1, R1]) { qe: QueryEvaluator[F, Q2, R2] => ds =>
      new Datasource[F, G, Q2, R2] {
        type PathType = ds.PathType
        val kind = ds.kind
        def evaluate(q: Q2) = qe.evaluate(q)
        def pathIsResource(p: ResourcePath) = ds.pathIsResource(p)
        def prefixedChildPaths(pfx: ResourcePath) = ds.prefixedChildPaths(pfx)
      }
    }
}
