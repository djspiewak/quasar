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

package quasar.impl.external

import quasar.contrib.fs2.convert
import slamdata.Predef._

import java.lang.{
  Class,
  ClassCastException,
  ClassLoader,
  ClassNotFoundException,
  ExceptionInInitializerError,
  IllegalAccessException,
  IllegalArgumentException,
  NoSuchFieldException,
  NullPointerException
}
import java.nio.file.{Files, Path}
import java.util.jar.JarFile

import argonaut.{JawnParser, Json}, JawnParser._
import cats.effect.{Blocker, ConcurrentEffect, ContextShift, Effect, Sync}
import cats.syntax.applicativeError._
import fs2.io.file
import fs2.{Chunk, Stream}
import jawnfs2._
import org.slf4s.Logging
import org.typelevel.jawn.AsyncParser

object ExternalModules extends Logging {
  import ExternalConfig._

  val PluginChunkSize = 8192

  def apply[F[_]: ConcurrentEffect: ContextShift](
      config: ExternalConfig,
      blocker: Blocker)(
      instantiate: PartialFunction[(PluginType, AnyRef), ExternalModule])
      : Stream[F, ExternalModule] = {

    val plugins = config match {
      case PluginDirectory(directory) =>
        Stream.eval(ConcurrentEffect[F].delay((Files.exists(directory), Files.isDirectory(directory)))) flatMap {
          case (true, true) =>
            convert.fromJavaStream(ConcurrentEffect[F].delay(Files.list(directory)))
              .filter(_.getFileName.toString.endsWith(PluginExtSuffix))
              .flatMap(loadPlugin[F](_, blocker))

          case (true, false) =>
            warnStream[F](s"Unable to load plugins from '$directory', does not appear to be a directory", None)

          case _ =>
            warnStream[F](s"Unable to load plugins from '$directory', does not exist", None)
        }

      case PluginFiles(files) =>
        Stream.emits(files)
          .flatMap(loadPlugin[F](_, blocker))

      case ExplodedDirs(modules) =>
        for {
          (cn, cp) <- Stream.emits(modules)
          classLoader <- Stream.eval(ClassPath.classLoader[F](ParentCL, cp))
        } yield (cn, classLoader, PluginType.Datasource)
    }

    def handle[A](className: ClassName, name: DisplayName, s: Stream[F, A]): Stream[F, A] =
      s recoverWith {
        case e @ (_: NoSuchFieldException | _: IllegalAccessException | _: IllegalArgumentException | _: NullPointerException) =>
          ExternalModules.warnStream[F](s"${name.uppercase} module '$className' does not appear to be a singleton object", Some(e))

        case e: ExceptionInInitializerError =>
          ExternalModules.warnStream[F](s"${name.uppercase} module '$className' failed to load with exception", Some(e))

        case _: ClassCastException =>
          ExternalModules.warnStream[F](s"${name.uppercase} module '$className' is not actually a subtype of ${name.uppercase}Module", None)
      }

    plugins flatMap {
      case (cn, classLoader, tpe) =>
        val base = for {
          clazz <- loadClass(cn, classLoader)
          back <- loadModule(clazz) collect {   // my kingdom for partial application
            case ar if instantiate.isDefinedAt((tpe, ar)) =>
              instantiate((tpe, ar))
          }
        } yield back

        handle(cn, tpe.displayName, base)
    }
  }

  private def loadModule[A, F[_]: Sync](clazz: Class[_]): Stream[F, AnyRef] =
    Stream.eval(Sync[F].delay(clazz.getDeclaredField("MODULE$").get(null)))

  private def loadClass[F[_]: Sync](cn: ClassName, classLoader: ClassLoader): Stream[F, Class[_]] =
    Stream.eval(Sync[F].delay(classLoader.loadClass(cn.value))) recoverWith {
      case cnf: ClassNotFoundException =>
        warnStream[F](s"Could not locate class for module '${cn.value}'", Some(cnf))
    }

  private def warnStream[F[_]: Sync](msg: => String, cause: Option[Throwable]): Stream[F, Nothing] =
    Stream.eval(Sync[F].delay(cause.fold(log.warn(msg))(log.warn(msg, _)))).drain

  private def infoStream[F[_]: Sync](msg: => String): Stream[F, Unit] =
    Stream.eval(Sync[F].delay(log.info(msg)))

  ////

  private def loadPlugin[F[_]: ContextShift: Effect](
      pluginFile: Path,
      blocker: Blocker)
      : Stream[F, (ClassName, ClassLoader, PluginType)] =
    for {
      plugin <- readPlugin(pluginFile, blocker)
      mainJar = new JarFile(plugin.mainJar.toFile)

      datasourceModuleAttr <- jarAttribute[F](mainJar, PluginType.Datasource.manifestAttributeName)
      destinationModuleAttr <- jarAttribute[F](mainJar, PluginType.Destination.manifestAttributeName)
      versionModuleAttr <- jarAttribute[F](mainJar, Plugin.ManifestVersionName)

      _ <- versionModuleAttr match {
        case None => warnStream[F](s"No '${Plugin.ManifestVersionName}' attribute found in Manifest for '$pluginFile'.", None)
        case Some(version) => infoStream[F](s"Loading $pluginFile with version $version")
      }

      moduleClasses <- (datasourceModuleAttr, destinationModuleAttr) match {
        case (Some(sourceModules), Some(destinationModules)) =>
          val datasourceClasses =
            sourceModules.split(" ").map((_, PluginType.Datasource))

          val destinationClasses =
            destinationModules.split(" ").map((_, PluginType.Destination))

          Stream.emit(datasourceClasses ++ destinationClasses)

        case (Some(sourceModules), None) =>
          Stream.emit(sourceModules.split(" ").map((_, PluginType.Datasource)))

        case (None, Some(destinationModules)) =>
          Stream.emit(destinationModules.split(" ").map((_, PluginType.Destination)))

        case _ =>
          warnStream[F](s"No '${PluginType.Datasource.manifestAttributeName}' or '${PluginType.Destination.manifestAttributeName}' attribute found in Manifest for '$pluginFile'.", None)
      }

      (moduleClass, pluginType) <- if (moduleClasses.isEmpty)
        warnStream[F](s"No classes defined for '${PluginType.Datasource.manifestAttributeName}' or '${PluginType.Destination.manifestAttributeName}' attributes in Manifest from '$pluginFile'.", None)
      else
        Stream.chunk(Chunk.array(moduleClasses)).covary[F]
      classLoader <- Stream.eval(ClassPath.classLoader[F](ParentCL, plugin.classPath))
    } yield (ClassName(moduleClass), classLoader, pluginType)

  private def readPlugin[F[_]: ContextShift: Effect](
      pluginFile: Path,
      blocker: Blocker)
      : Stream[F, Plugin] =
    for {
      js <-
        file.readAll[F](pluginFile, blocker, PluginChunkSize)
          .chunks
          .map(_.toByteBuffer)
          .parseJson[Json](AsyncParser.SingleValue)

      pluginResult <- Stream.eval(Plugin.fromJson[F](js))

      plugin <- pluginResult.fold(
        (s, c) => warnStream[F](s"Failed to decode plugin from '$pluginFile': $s ($c)", None),
        r => Stream.eval(r.withAbsolutePaths[F](pluginFile.getParent)))
    } yield plugin

  private val ParentCL = this.getClass.getClassLoader

  private val PluginExtSuffix = "." + Plugin.FileExtension

  private def jarAttribute[F[_]: Sync](j: JarFile, attr: String): Stream[F, Option[String]] =
    Stream.eval(Sync[F].delay(Option(j.getManifest.getMainAttributes.getValue(attr))))
}
