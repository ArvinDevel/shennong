/**
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
package org.apache.spark.sql.cstream

import java.util.concurrent.{ConcurrentMap, ExecutionException, TimeUnit}

import scala.util.control.NonFatal
import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.bookkeeper.clients.config.StorageClientSettings
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

case class ClientConfig(url: String, namespace: String)

private[pulsar] object CachedStorageClient extends Logging {

  private type Client = org.apache.bookkeeper.api.StorageClient

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long =
    Option(SparkEnv.get).map(_.conf.getTimeAsMs(
      "spark.bookkeeper.client.cache.timeout",
      s"${defaultCacheExpireTimeout}ms")).getOrElse(defaultCacheExpireTimeout)

  private val cacheLoader = new CacheLoader[ClientConfig, Client] {
    override def load(streamName: ClientConfig): Client = {
      createClient(streamName)
    }
  }

  private val removalListener = new RemovalListener[ClientConfig, Client]() {
    override def onRemoval(
        notification: RemovalNotification[ClientConfig, Client]): Unit = {
      val sn: ClientConfig = notification.getKey
      val client: Client = notification.getValue
      logDebug(
        s"Evicting $client $sn, due to ${notification.getCause}")
      close(sn, client)
    }
  }

  private lazy val guavaCache: LoadingCache[ClientConfig, Client] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[ClientConfig, Client](cacheLoader)


  private def createClient(config: ClientConfig): Client = {

    logInfo(s"Client Conf = ${config}")
    try {
      val storageClient: Client = org.apache.bookkeeper.clients.StorageClientBuilder.newBuilder()
        .withSettings(StorageClientSettings.newBuilder().serviceUri(config.url).build())
        .withNamespace(config.namespace)
        .build();
      logDebug(s"Created a new instance of StorageClient for ClientConfig = $config.")
      storageClient
    } catch {
      case e: Throwable =>
        logError(s"Failed to create PulsarStream using Client conf ${config}", e)
        throw e
    }
  }

  /**
    * Get a cached PulsarProducer for a given configuration. If matching PulsarProducer doesn't
    * exist, a new PulsarProducer will be created. PulsarProducer is thread safe, it is best to keep
    * one instance per specified pulsarParams.
    */
  private[pulsar] def getOrCreate(config: ClientConfig): Client = {
    try {
      guavaCache.get(config)
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null =>
        throw e.getCause
    }
  }

  /** For explicitly closing pulsar producer */
  private[pulsar] def close(config: ClientConfig): Unit = {
    guavaCache.invalidate(config)
  }

  /** Auto close on cache evict */
  private def close(config: ClientConfig, Client: Client): Unit = {
    try {
      logInfo(s"Closing the Client: $config.")
      Client.close()
    } catch {
      case NonFatal(e) => logWarning("Error while closing pulsar producer.", e)
    }
  }

  private[pulsar] def clear(): Unit = {
    logInfo("Cleaning up guava cache.")
    guavaCache.invalidateAll()
  }

  // Intended for testing purpose only.
  private def getAsMap: ConcurrentMap[ClientConfig, Client] = guavaCache.asMap()
}
