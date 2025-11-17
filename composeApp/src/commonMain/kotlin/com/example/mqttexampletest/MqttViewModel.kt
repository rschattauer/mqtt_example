package com.example.mqttexampletest

import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import de.kempmobil.ktor.mqtt.MqttClient
import de.kempmobil.ktor.mqtt.SubscriptionIdentifier
import de.kempmobil.ktor.mqtt.Topic
import de.kempmobil.ktor.mqtt.TopicFilter
import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.WebSockets
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.retryWhen
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.io.bytestring.decodeToString

class MqttViewModel : ViewModel() {

    private companion object {
        private const val RETRY_TIMEOUT_IN_MS = 3_000L
        private const val BROKER_URL = "wss://test.mosquitto.org:8081/mqtt"
    }

    // Expose a shared client flow that connects to the public Mosquitto test broker over secure WebSockets.
    // Uses anonymous access as requested.
    private val mqttClientFlow: Flow<Result<MqttClient>> = callbackFlow {
        // Build client with platform-appropriate Ktor engine (OkHttp on Android, Darwin on iOS)
        val client = de.kempmobil.ktor.mqtt.ws.MqttClient(BROKER_URL) {
            clientId = "mqttClientId1234"
            // Use Default dispatcher for KMP compatibility
            dispatcher = Dispatchers.IO
            // Anonymous access (no username/password)
            connection {
                http = {
                    HttpClient {
                        install(WebSockets)
                    }
                }
            }
        }

        val connack = client.connect().getOrNull()
        if (connack?.isSuccess != true) {
            close(IllegalStateException("Failed to connect to mqtt client: ${connack?.reason} - ${connack?.reasonString}"))
            return@callbackFlow
        }

        try {
            trySend(Result.success(client))
        } catch (t: Throwable) {
            // If sending fails, disconnect the client and close
            viewModelScope.launch { client.disconnect() }
            close(t)
        }

        awaitClose {
            // Cleanly disconnect when no longer collected
            viewModelScope.launch {
                try {
                    client.disconnect()
                } catch (_: Throwable) {
                }
            }
        }
    }
        .retryWhen { _, attempt ->
            // retry a couple of times on failure
            delay(RETRY_TIMEOUT_IN_MS)
            return@retryWhen attempt < 2 // Stop trying after the third time
        }
        .catch { throwable ->
            emit(Result.failure(throwable))
        }

    // Share the client so multiple topic subscriptions reuse a single connection
    private val sharedClientFlow = mqttClientFlow
        .shareIn(
            viewModelScope,
            SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000),
            replay = 1
        )

    // Helper: subscribe to a topic and emit String payloads
    private fun subscribeAsString(identifier: Int, topic: String): Flow<String> =
        sharedClientFlow.flatMapLatest { result ->
            val subscriptionIdentifier = SubscriptionIdentifier(identifier)
            result.fold(
                onSuccess = { client ->
                    client.subscribe(
                        filters = listOf(TopicFilter(filter = Topic(topic))),
                        subscriptionIdentifier = subscriptionIdentifier
                    )
                    client.publishedPackets
                        .filter { it.subscriptionIdentifier == subscriptionIdentifier }
                        .map { runCatching { it.payload.decodeToString() }.getOrElse { "" } }
                        .onStart { emit("Connected") }
                },
                onFailure = { error ->
                    flow { emit("ERROR: ${error.message ?: error::class.simpleName}") }
                }
            )
        }
            .catch { throwable ->
                Log.e("Socket", "Error while subscribing to topic $topic: $throwable")
            }

    // Three StateFlows that keep the connection alive while they are observed
    val topicOne: StateFlow<String> =
        subscribeAsString(identifier = 1, topic = "test/one").stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000),
            initialValue = ""
        )

    val topicTwo: StateFlow<String> =
        subscribeAsString(identifier = 2, topic = "test/two").stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000),
            initialValue = ""
        )

    val topicThree: StateFlow<String> =
        subscribeAsString(identifier = 3, topic = "test/three").stateIn(
            scope = viewModelScope,
            started = SharingStarted.WhileSubscribed(stopTimeoutMillis = 5_000),
            initialValue = ""
        )
}
