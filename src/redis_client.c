/**
 * @file redis_client.c
 * @brief Asynchronous Redis client based on hiredis + libevent with
 * auto-reconnect
 *
 * Core features:
 * 1. Asynchronous connection to Redis server
 * 2. Automatic reconnection on disconnect (exponential backoff strategy)
 * 3. Support for user business callbacks (executed after connection is ready)
 * 4. Thread-safe event-driven model
 */


#include "redis_client.h"
#include <event2/util.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* Usage flow:
 * 1. redis_client_create()     - Create client instance
 * 2. redis_client_set_callbacks() - Set callback functions
 * 3. redis_client_connect()    - Initiate connection (with auto-reconnect)
 * 4. redis_client_run()        - Run event loop
 * 5. redis_client_free()       - Free resources
 */

#ifdef _WIN32
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <unistd.h>
#endif

// ============================================================================
// Thread Safety Macros
// ============================================================================

#ifdef _WIN32
#define REDIS_CLIENT_LOCK(client) EnterCriticalSection(&(client)->cs)
#define REDIS_CLIENT_UNLOCK(client) LeaveCriticalSection(&(client)->cs)
#else
#define REDIS_CLIENT_LOCK(client) pthread_mutex_lock(&(client)->mutex)
#define REDIS_CLIENT_UNLOCK(client) pthread_mutex_unlock(&(client)->mutex)
#endif

// ============================================================================
// State Machine Transition Functions
// ============================================================================
// Each function corresponds to one row in the state transition matrix:
//
// | Current State | Event               | Action                          | New State    |
// |---------------|---------------------|---------------------------------|--------------|
// | DISCONNECTED  | connect/reconnect    | Initiate connection            | CONNECTING   |
// | CONNECTING    | connection_ok        | Start periodic/keepalive       | CONNECTED    |
// | CONNECTING    | connection_fail      | Start reconnect timer (backoff)| DISCONNECTED |
// | CONNECTING    | watchdog_timeout     | Force cleanup + reconnect      | DISCONNECTED |
// | CONNECTED     | disconnect           | Start reconnect timer          | DISCONNECTED |
//
// Naming convention: _sm_[from_state]_[to_state]()
// ============================================================================

/**
 * State transition: DISCONNECTED → CONNECTING
 * Event: Connection requested (via connect() or reconnect timer)
 * Action: Set reconnecting flag, initiate connection, start watchdog
 */
static void _sm_disconnected_to_connecting(RedisClient *client);

/**
 * State transition: CONNECTING → CONNECTED
 * Event: Connection completed successfully
 * Action: Reset reconnect count, start periodic timer, call user callbacks
 */
static void _sm_connecting_to_connected(RedisClient *client);

/**
 * State transition: CONNECTING → DISCONNECTED
 * Event: Connection failed or watchdog timeout
 * Action: Cleanup failed connection, start reconnect timer
 */
static void _sm_connecting_to_disconnected(RedisClient *client, const char *error_msg);

/**
 * State transition: CONNECTED → DISCONNECTED
 * Event: Connection lost
 * Action: Cleanup connection, start reconnect timer
 */
static void _sm_connected_to_disconnected(RedisClient *client);

// ============================================================================
// Internal function declarations (in call order)
// ============================================================================

/**
 * @brief Safely duplicate string (deep copy)
 * @param src Source string (can be NULL)
 * @return Newly allocated string, or NULL
 */
static char *_safe_strdup(const char *src);

/**
 * @brief Connection completion event - called when hiredis async connection
 * completes
 * @param ac Redis async context
 * @param status Connection status (REDIS_OK indicates success)
 */
static void _on_connection_completed(const redisAsyncContext *ac, int status);

/**
 * @brief Connection lost event - called when Redis connection is disconnected
 * @param ac Redis async context
 * @param status Disconnection status
 */
static void _on_connection_lost(const redisAsyncContext *ac, int status);

/**
 * @brief Configure and start reconnect timer - set delay before retrying
 * connection
 * @param client Redis client instance
 * @param immediate If non-zero, execute connection immediately (0ms delay)
 * @note Uses exponential backoff strategy: 1s, 2s, 4s, ..., max 30s
 */
static void _configure_and_start_reconnect_timer(RedisClient *client,
                                                 int immediate);

/**
 * @brief Reconnect timer timeout callback - triggers async connection setup on
 * timer
 * @param fd File descriptor (libevent parameter, unused)
 * @param events Event type (libevent parameter, unused)
 * @param arg User data (RedisClient pointer)
 */
static void _on_reconnect_timeout(evutil_socket_t fd, short events, void *arg);

/**
 * @brief Configure and start periodic task timer - set timer parameters and
 * start
 * @param client Redis client instance
 */
static void _configure_and_start_periodic_timer(RedisClient *client);

/**
 * @brief Periodic task timer timeout callback - triggers periodic task
 * execution on timer
 * @param fd File descriptor (libevent parameter, unused)
 * @param events Event type (libevent parameter, unused)
 * @param arg User data (RedisClient pointer)
 */
static void _on_periodic_timeout(evutil_socket_t fd, short events, void *arg);

/**
 * @brief Configure and start keepalive timer - ensure event loop stays active
 * @param client Redis client instance
 */
static void _configure_and_start_keepalive_timer(RedisClient *client);

/**
 * @brief Keepalive timer timeout callback - keeps event loop active
 * @param fd File descriptor (libevent parameter, unused)
 * @param events Event type (libevent parameter, unused)
 * @param arg User data (RedisClient pointer)
 */
static void _on_keepalive_timeout(evutil_socket_t fd, short events, void *arg);

/**
 * @brief Start connection watchdog timer - detect when connection takes too
 * long
 *
 * This timer is started when actual connection begins (in
 * _on_reconnect_timeout), NOT when redis_client_connect() is called. It
 * monitors the connection process.
 *
 * Timeline:
 * 1. redis_client_connect() - creates event_loop and timer objects (not
 * started)
 * 2. _on_reconnect_timeout() - starts actual connection and this timer
 * 3. _on_connection_watchdog_timeout() - triggers if connection not complete in
 * 10s
 *
 * @param client Redis client instance
 */
static void _configure_and_start_connection_watchdog_timer(RedisClient *client);

/**
 * @brief Connection watchdog timeout callback - triggers when connection takes
 * too long
 *
 * This timer is started when actual connection attempt begins (in
 * _on_reconnect_timeout), not when redis_client_connect() is called. It
 * monitors the connection process to detect if the connection hangs for more
 * than 10 seconds.
 *
 * @param fd File descriptor (libevent parameter, unused)
 * @param events Event type (libevent parameter, unused)
 * @param arg User data (RedisClient pointer)
 */
static void _on_connection_watchdog_timeout(evutil_socket_t fd, short events,void *arg);

// ============================================================================
// Public API implementation
// ============================================================================

/**
 * @brief Create Redis client instance, which is a custom structure
 *
 * @param host Redis server address (e.g., "127.0.0.1")
 * @param port Redis server port (e.g., 6379)
 * @param password Authentication password (NULL means no authentication)
 * @return RedisClient* Returns instance pointer on success, NULL on failure
 *
 * @note After creation, need to call redis_client_connect() to start connection
 * @note Must call redis_client_free() to release resources after use
 */
RedisClient *redis_client_create(const char *host, int port,const char *password) {
  // Parameter validation
  if (!host || port <= 0 || port > 65535) {
    printf("[ERROR] Invalid address or port: host=%s, port=%d\n",
           host ? host : "NULL", port);
    return NULL;
  }

  // Allocate memory and initialize
  RedisClient *client = (RedisClient *)malloc(sizeof(RedisClient));
  if (!client) {
    printf("[ERROR] Memory allocation failed\n");
    return NULL;
  }
  memset(client, 0, sizeof(RedisClient));

  // Save connection configuration
  client->host = _safe_strdup(host);
  if (!client->host) {
    free(client);
    return NULL;
  }

  client->port = port;

  if (password) {
    client->password = _safe_strdup(password);
    if (!client->password) {
      free(client->host);
      free(client);
      return NULL;
    }
  }

  // Initialize event loop and timers to NULL (will be created when connecting)
  // Note: memset already set these to NULL, but explicit initialization
  // improves code clarity
  client->event_loop = NULL;                // Event loop (created in connect)
  client->keepalive_timer = NULL;           // Keepalive timer
  client->reconnect_timer = NULL;           // Reconnect timer
  client->connection_watchdog_timer = NULL; // Connection watchdog timer
  client->periodic_timer = NULL;            // Periodic task timer
  printf("[INFO] Client initialized, event loop and timers will be created on "
         "connect\n");

  // Initialize default configuration
  client->reconnect_delay_ms = 1000;
  client->reconnect_attempts = 0;
  client->state = RC_DISCONNECTED;
  client->client_type = RCT_UNKNOWN; // Initialize client type
  client->is_reconnecting = 0;       // Initialize reconnection flag

  // Initialize thread safety lock
#ifdef _WIN32
  InitializeCriticalSection(&client->cs);
#else
  pthread_mutex_init(&client->mutex, NULL);
#endif

  printf("[INFO] Created client: %s:%d\n", host, port);
  return client;
}

/**
 * @brief Free Redis client instance and all its resources
 *
 * @param client Client instance to free
 *
 * @note Safety feature: function handles NULL parameters
 * @note Cleanup order: timers → Redis connection → event loop → string
 * resources → memory
 */
void redis_client_free(RedisClient *client) {
  if (!client) {
    printf("redis_client_free!client   error!");
    return;
  }

  printf("[INFO] Freeing client resources\n");

  // 0. Destroy thread safety lock
#ifdef _WIN32
  DeleteCriticalSection(&client->cs);
#else
  pthread_mutex_destroy(&client->mutex);
#endif

  // 1. Clean up keepalive timer
  if (client->keepalive_timer) {
    event_del(client->keepalive_timer);  // Step 1: Remove timer from event loop
    event_free(client->keepalive_timer); // Step 2: Free timer object memory
    client->keepalive_timer = NULL;      // Step 3: Set pointer to NULL
  }

  // 2. Clean up periodic task timer
  if (client->periodic_timer) {
    event_del(client->periodic_timer);  // Step 1: Remove timer from event loop
    event_free(client->periodic_timer); // Step 2: Free timer object memory
    client->periodic_timer = NULL;      // Step 3: Set pointer to NULL
  }

  // 3. Clean up reconnect timer
  if (client->reconnect_timer) {
    event_del(client->reconnect_timer);  // Step 1: Remove timer from event loop
    event_free(client->reconnect_timer); // Step 2: Free timer object memory
    client->reconnect_timer = NULL;      // Step 3: Set pointer to NULL
  }

  // 4. Clean up connection watchdog timer
  if (client->connection_watchdog_timer) {
    event_del(client->connection_watchdog_timer);
    event_free(client->connection_watchdog_timer);
    client->connection_watchdog_timer = NULL;
  }

  // 2. Clean up Redis connection
  if (client->redis_ctx) {
    redisAsyncContext *old_ctx = client->redis_ctx;
    client->redis_ctx->data = NULL;
    client->redis_ctx = NULL;
    redisAsyncFree(old_ctx);
  }

  // 3. Clean up event loop
  if (client->event_loop) {
    event_base_free(client->event_loop);
    client->event_loop = NULL;
  }

  // 4. Clean up string resources
  if (client->host) {
    free(client->host);
    client->host = NULL;
  }

  if (client->password) {
    free(client->password);
    client->password = NULL;
  }

  // 5. Clean up the structure itself
  free(client);
}

/**
 * @brief Set user-defined callback functions
 *
 * @param client Redis client instance
 * @param on_connected Connection success callback (optional)
 * @param on_ready Connection ready callback (required) - for executing user
 * business code
 * @param on_disconnected Connection disconnected callback (optional)
 *
 * @note on_ready callback is called when connection is successful and ready to
 * receive commands
 * @note After reconnection succeeds, on_ready will be called again to restore
 * business state
 */
void redis_client_set_callbacks(RedisClient *client,
                                RedisConnectCallback on_connected,
                                RedisConnectCallback on_ready,
                                RedisDisconnectCallback on_disconnected,
                                RedisPeriodicCallback on_periodic,
                                int periodic_interval_ms) {
  if (!client) {
    printf("redis_client_set_callbacks  !client   error!");
    return;
  }
  printf("[INFO] Setting callback functions\n");
  client->on_connected = on_connected;
  client->on_ready = on_ready;
  client->on_disconnected = on_disconnected;
  client->on_periodic = on_periodic;
  client->periodic_interval_ms = periodic_interval_ms;
}

/**
 * @brief Check if client is in connected state
 *
 * @param client Redis client instance
 * @return int 1 means connected, 0 means not connected
 *
 * @note Connection state is determined by client->state
 * @note RC_CONNECTED is the only state considered as connected
 */
int redis_client_is_connected(RedisClient *client) {
  if (!client) {
    printf("redis_client_is_connected  !client   error!");
    return 0;
  }

  // Thread-safe: Check state with lock
  REDIS_CLIENT_LOCK(client);
  int is_connected = (client->state == RC_CONNECTED);
  REDIS_CLIENT_UNLOCK(client);
  return is_connected;
}

/**
 * @brief Set client type
 *
 * @param client Redis client instance
 * @param type Client type (RCT_PUBLISHER/RCT_SUBSCRIBER/RCT_UNKNOWN)
 */
void redis_client_set_type(RedisClient *client, RedisClientType type) {
  if (!client) {
    printf("redis_client_set_type  !client   error!");
    return;
  }

  REDIS_CLIENT_LOCK(client);
  client->client_type = type;
  REDIS_CLIENT_UNLOCK(client);

  const char *type_str = "UNKNOWN";
  switch (type) {
  case RCT_PUBLISHER:
    type_str = "PUBLISHER";
    break;
  case RCT_SUBSCRIBER:
    type_str = "SUBSCRIBER";
    break;
  default:
    type_str = "UNKNOWN";
    break;
  }
  printf("[INFO] Client type set to: %s\n", type_str);
}

// ============================================================================
// State Machine Transition Implementation
// ============================================================================

/**
 * @brief State transition: DISCONNECTED → CONNECTING
 * @details Called when a connection is requested (via connect() or reconnect timer).
 *          Sets reconnecting flag, initiates connection, and starts watchdog.
 *
 * Matrix row: DISCONNECTED + connect/reconnect → CONNECTING
 *
 * @param client Redis client instance
 */
static void _sm_disconnected_to_connecting(RedisClient *client) {
  printf("[SM] DISCONNECTED → CONNECTING: transition triggered\n");

  // Set state to connecting
  REDIS_CLIENT_LOCK(client);
  client->is_reconnecting = 1;
  client->state = RC_CONNECTING;
  REDIS_CLIENT_UNLOCK(client);

  // Clean up old connection (if exists)
  if (client->redis_ctx) {
    redisAsyncContext *old_ctx = client->redis_ctx;
    client->redis_ctx->data = NULL;
    client->redis_ctx = NULL;
    redisAsyncFree(old_ctx);
  }

  // Create new connection
  client->redis_ctx = redisAsyncConnect(client->host, client->port);

  // Handle synchronous failure (immediate failure during context creation)
  if (!client->redis_ctx || client->redis_ctx->err) {
    printf("[ERROR] Synchronous connection failure for client %p: %s\n",
           (void *)client,
           client->redis_ctx ? (client->redis_ctx->errstr ? client->redis_ctx->errstr : "unknown error")
                             : "failed to create async context");
    fflush(stdout);

    if (client->redis_ctx) {
      redisAsyncContext *failed_ctx = client->redis_ctx;
      client->redis_ctx->data = NULL;
      client->redis_ctx = NULL;
      redisAsyncFree(failed_ctx);
    }

    // Transition back to DISCONNECTED (this will trigger reconnect)
    REDIS_CLIENT_LOCK(client);
    client->state = RC_DISCONNECTED;
    client->is_reconnecting = 0;
    REDIS_CLIENT_UNLOCK(client);

    // Ensure keepalive timer keeps running during reconnection
    _configure_and_start_keepalive_timer(client);
    
    _configure_and_start_reconnect_timer(client, 0);
    return;
  }

  // Set up connection callbacks
  client->redis_ctx->data = client;
  redisLibeventAttach(client->redis_ctx, client->event_loop);
  redisAsyncSetConnectCallback(client->redis_ctx, _on_connection_completed);
  redisAsyncSetDisconnectCallback(client->redis_ctx, _on_connection_lost);

  // Ensure keepalive timer keeps running during connection attempt
  _configure_and_start_keepalive_timer(client);
  
  // Start connection watchdog timer
  _configure_and_start_connection_watchdog_timer(client);
}

/**
 * @brief State transition: CONNECTING → CONNECTED
 * @details Called when connection completes successfully.
 *          Resets reconnect counter, starts periodic timer, invokes user callbacks.
 *
 * Matrix row: CONNECTING + connection_ok → CONNECTED
 *
 * @param client Redis client instance
 */
static void _sm_connecting_to_connected(RedisClient *client) {
  printf("[SM] CONNECTING → CONNECTED: transition triggered\n");

  // Update state and reset reconnect parameters (thread-safe)
  REDIS_CLIENT_LOCK(client);
  client->state = RC_CONNECTED;
  client->is_reconnecting = 0;
  client->reconnect_attempts = 0;
  client->reconnect_delay_ms = 1000;
  REDIS_CLIENT_UNLOCK(client);

  // Call user on_connected callback
  if (client->on_connected) {
    client->on_connected(client);
    fflush(stdout);
  }

  // Call user on_ready callback and start periodic timer
  if (client->on_ready) {
    printf("[INFO] Connection ready, executing user business code\n");
    fflush(stdout);

    // Start periodic task timer when connection is ready
    // Ensure timer starts only when connection is truly available
    if (client->on_periodic) {
      _configure_and_start_periodic_timer(client);
    }

    client->on_ready(client);
    fflush(stdout);
  }

  // Ensure keepalive timer keeps running
  _configure_and_start_keepalive_timer(client);
}

/**
 * @brief State transition: CONNECTING → DISCONNECTED
 * @details Called when connection fails or watchdog timeout occurs.
 *          Cleans up failed connection and starts reconnect timer.
 *
 * Matrix row: CONNECTING + connection_fail → DISCONNECTED
 * Matrix row: CONNECTING + watchdog_timeout → DISCONNECTED
 *
 * @param client Redis client instance
 * @param error_msg Error message (can be NULL)
 */
static void _sm_connecting_to_disconnected(RedisClient *client, const char *error_msg) {
  if (error_msg) {
    printf("[SM] CONNECTING → DISCONNECTED: %s\n", error_msg);
  } else {
    printf("[SM] CONNECTING → DISCONNECTED: transition triggered\n");
  }
  fflush(stdout);

  // Cleanup failed redis_ctx
  if (client->redis_ctx) {
    printf("[DEBUG] Freeing failed redis_ctx: %p\n", (void *)client->redis_ctx);
    redisAsyncContext *failed_ctx = client->redis_ctx;
    client->redis_ctx->data = NULL;
    client->redis_ctx = NULL;
    redisAsyncFree(failed_ctx);
  }

  // Update state (thread-safe)
  REDIS_CLIENT_LOCK(client);
  client->state = RC_DISCONNECTED;
  client->is_reconnecting = 0;
  REDIS_CLIENT_UNLOCK(client);

  // Call user on_disconnected callback
  if (client->on_disconnected) {
    client->on_disconnected(client);
    fflush(stdout);
  }

  // Ensure keepalive timer keeps running during reconnection
  _configure_and_start_keepalive_timer(client);

  // Start reconnect timer (with exponential backoff)
  printf("[DEBUG] Starting reconnect timer...\n");
  fflush(stdout);
  _configure_and_start_reconnect_timer(client, 0);
}

/**
 * @brief State transition: CONNECTED → DISCONNECTED
 * @details Called when connection is lost (normal disconnect).
 *          Cleans up connection and starts reconnect timer.
 *
 * Matrix row: CONNECTED + disconnect → DISCONNECTED
 *
 * @param client Redis client instance
 */
static void _sm_connected_to_disconnected(RedisClient *client) {
  printf("[SM] CONNECTED → DISCONNECTED: transition triggered\n");

  // Cleanup connection
  if (client->redis_ctx) {
    redisAsyncContext *old_ctx = client->redis_ctx;
    client->redis_ctx->data = NULL;  // Disconnect association first
    client->redis_ctx = NULL;        // Then nullify field
    redisAsyncFree(old_ctx);         // Finally free
  }

  // Update state (thread-safe)
  REDIS_CLIENT_LOCK(client);
  client->state = RC_DISCONNECTED;
  client->is_reconnecting = 0;  // Not in connecting state
  REDIS_CLIENT_UNLOCK(client);

  // Call user on_disconnected callback
  if (client->on_disconnected) {
    client->on_disconnected(client);
  }

  // Ensure keepalive timer keeps running
  _configure_and_start_keepalive_timer(client);

  // Start reconnect timer
  _configure_and_start_reconnect_timer(client, 0);
}

// ============================================================================
// Internal helper function implementation
// ============================================================================

/**
 * @brief Safely duplicate string (deep copy)
 *
 * @param src Source string (can be NULL)
 * @return char* Newly allocated string, or NULL
 *
 * @note Allocated memory needs to be freed with free()
 * @note Returns NULL if src is NULL
 */
static char *_safe_strdup(const char *src) {
  if (!src) {
    printf("the string  dulplicated is  NULL!\n");
    return NULL;
  }

  size_t len = strlen(src) + 1; // Include null terminator '\0'
  char *dst = (char *)malloc(len);
  if (!dst) {
    printf("[ERROR] String memory allocation failed: need %zu bytes\n", len);
    return NULL;
  }

  // Use memcpy instead of snprintf to avoid formatting issues
  memcpy(dst, src, len);

  printf("[DEBUG] Copied string: '%s' (%zu bytes)\n", src, len);
  return dst;
}

/**
 * @brief Connection callback - called by hiredis when async connection
 * completes
 *
 * @details This function only performs validation and dispatches to the
 *          appropriate state machine transition function based on the result.
 *
 * Dispatch table:
 *   - status == REDIS_OK  → _sm_connecting_to_connected()
 *   - status != REDIS_OK  → _sm_connecting_to_disconnected()
 *
 * @param ac Redis async context
 * @param status Connection status: REDIS_OK indicates success, otherwise
 * failure
 */
static void _on_connection_completed(const redisAsyncContext *ac, int status) {
  RedisClient *client = (RedisClient *)(ac ? ac->data : NULL);
  if (!client) {
    printf(" _on_connection_completed  !client   error!");
    return;
  }

  printf("[DEBUG] Connection callback triggered for client %p, status=%d\n",
         (void *)client, status);
  fflush(stdout);

  // Thread-safe: Check if this callback is still valid (prevent race with
  // timeout)
  REDIS_CLIENT_LOCK(client);

  // Check if connection state was already set to DISCONNECTED by watchdog
  if (client->state == RC_DISCONNECTED) {
    printf("[DEBUG] Connection callback ignored: connection already cleaned up by timeout\n");
    REDIS_CLIENT_UNLOCK(client);
    return;
  }

  // Clear connection watchdog timer since connection has completed (success or failure)
  if (client->connection_watchdog_timer) {
    event_del(client->connection_watchdog_timer);
  }

  REDIS_CLIENT_UNLOCK(client);

  // Dispatch to state machine transition based on connection result
  if (status == REDIS_OK) {
    // Matrix row: CONNECTING + connection_ok → CONNECTED
    printf("[SUCCESS] Redis connection successful for client %p\n", (void *)client);
    _sm_connecting_to_connected(client);
  } else {
    // Matrix row: CONNECTING + connection_fail → DISCONNECTED
    printf("[ERROR] Connection failed for client %p: %s (status=%d)\n",
           (void *)client, ac->errstr ? ac->errstr : "Unknown error", status);
    _sm_connecting_to_disconnected(client, ac->errstr ? ac->errstr : "Unknown error");
  }
}

/**
 * @brief Disconnection callback - called by hiredis when connection is
 * disconnected
 *
 * @details This function performs validation and dispatches to state machine.
 *          Only handles disconnects from CONNECTED state.
 *
 * Matrix row: CONNECTED + disconnect → DISCONNECTED
 *
 * @param ac Redis async context
 * @param status Disconnection status
 */
static void _on_connection_lost(const redisAsyncContext *ac, int status) {
  RedisClient *client = ac ? (RedisClient *)(ac->data) : NULL;
  if (!client) return;

  REDIS_CLIENT_LOCK(client);

  // Prevent duplicate processing: if already disconnected, skip
  if (client->state == RC_DISCONNECTED) {
    REDIS_CLIENT_UNLOCK(client);
    return;
  }

  // Only process disconnect if currently connected
  if (client->state != RC_CONNECTED) {
    REDIS_CLIENT_UNLOCK(client);
    printf("[WARN] _on_connection_lost called in unexpected state: %d\n", client->state);
    return;
  }

  REDIS_CLIENT_UNLOCK(client);

  // Dispatch to state machine transition
  // Matrix row: CONNECTED + disconnect → DISCONNECTED
  _sm_connected_to_disconnected(client);
}

/**
 * @brief Keepalive timer callback - keeps event loop active
 *
 * @param fd File descriptor (libevent parameter, unused)
 * @param events Event type (libevent parameter, unused)
 * @param arg User data (RedisClient pointer)
 */
static void _on_keepalive_timeout(evutil_socket_t fd, short events, void *arg) {
  RedisClient *client = (RedisClient *)arg;
  if (!client || !client->event_loop) {
    return;
  }

  // Print heartbeat with client type
  const char *type = (client->client_type == RCT_PUBLISHER)    ? "PUB"
                     : (client->client_type == RCT_SUBSCRIBER) ? "SUB"
                                                               : "???";
  printf("[HEARTBEAT] %s client %p: loop active, state=%s\n", type,
         (void *)client,
         client->state == RC_CONNECTED    ? "CONNECTED"
         : client->state == RC_CONNECTING ? "CONNECTING"
                                          : "DISCONNECTED");
  fflush(stdout);

  // Restart keepalive timer to keep event loop active
  _configure_and_start_keepalive_timer(client);
}

/**
 * @brief Connection watchdog timeout callback - triggers when connection takes
 * too long
 *
 * @details This function detects stuck connections and forces state transition.
 *          Only triggers when in CONNECTING state with reconnecting flag set.
 *
 * Matrix row: CONNECTING + watchdog_timeout → DISCONNECTED
 */
static void _on_connection_watchdog_timeout(evutil_socket_t fd, short events,void *arg) {
  RedisClient *client = (RedisClient *)arg;
  if (!client) {
    return;
  }

  REDIS_CLIENT_LOCK(client);

  // Quick check: only process if in CONNECTING state with reconnecting flag
  // (connection might have completed just before this callback fired)
  if (client->state != RC_CONNECTING || !client->is_reconnecting) {
    REDIS_CLIENT_UNLOCK(client);
    return;
  }

  REDIS_CLIENT_UNLOCK(client);

  printf("[WARN] Connection watchdog timeout, forcing state transition\n");
  fflush(stdout);

  // Dispatch to state machine transition
  // Matrix row: CONNECTING + watchdog_timeout → DISCONNECTED
  _sm_connecting_to_disconnected(client, "watchdog timeout");
}

/**
 * @brief Periodic task timer callback - triggers periodic task execution on
 * timer
 *
 * @param fd File descriptor (libevent parameter, unused)
 * @param events Event type (libevent parameter, unused)
 * @param arg User data (RedisClient pointer)
 */
static void _on_periodic_timeout(evutil_socket_t fd, short events, void *arg) {
  RedisClient *client = (RedisClient *)arg;
  if (!client) {
    printf("_on_periodic_timeout  !client   error!");
    return;
  }
  // Check if client event loop still exists (prevents access during cleanup)
  if (!client->event_loop) {
    return;
  }
  // Only execute periodic task when connected
  if (client->state == RC_CONNECTED && client->on_periodic) {
    client->on_periodic(client);
  }
  // Print periodic task trigger
  const char *type = (client->client_type == RCT_PUBLISHER)    ? "PUB"
                     : (client->client_type == RCT_SUBSCRIBER) ? "SUB"
                                                               : "???";
  printf("[PERIODIC] %s client %p: task triggered, state=%s\n", type,
         (void *)client,
         client->state == RC_CONNECTED ? "CONNECTED" : "DISCONNECTED");
  fflush(stdout);
  // Restart timer (loop execution)
  _configure_and_start_periodic_timer(client);
}
/**
 * @brief Start keepalive timer - ensure event loop stays active
 *
 * @param client Redis client instance
 */
static void _configure_and_start_keepalive_timer(RedisClient *client) {
  if (!client || !client->event_loop || !client->keepalive_timer) {
    return;
  }

  // Remove existing timer (safe if not in event loop)
  event_del(client->keepalive_timer);

  // Start keepalive timer (5 second interval)
  struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
  evtimer_add(client->keepalive_timer, &tv);
}

/**
 * @brief Start periodic task - add periodic task timer to event loop
 *
 * @param client Redis client instance
 */
static void _configure_and_start_periodic_timer(RedisClient *client) {
  if (!client || !client->event_loop || !client->periodic_timer) {
    return;
  }
  if (!client->on_periodic || client->periodic_interval_ms <= 0) {
    return;
  }

  // Remove existing timer (safe if not in event loop)
  event_del(client->periodic_timer);

  // Start periodic timer
  struct timeval tv = {.tv_sec = client->periodic_interval_ms / 1000,
                       .tv_usec = (client->periodic_interval_ms % 1000) * 1000};
  evtimer_add(client->periodic_timer, &tv);
}

/**
 * @brief Start connection watchdog timer - detect when connection takes too
 * long
 *
 * This timer is started when actual connection begins (in
 * _on_reconnect_timeout), NOT when redis_client_connect() is called. It
 * monitors the connection process.
 *
 * Timeline:
 * 1. redis_client_connect() - creates event_loop and timer objects (not
 * started)
 * 2. _on_reconnect_timeout() - starts actual connection and this timer
 * 3. _on_connection_watchdog_timeout() - triggers if connection not complete in
 * 10s
 *
 * @param client Redis client instance
 */
static void
_configure_and_start_connection_watchdog_timer(RedisClient *client) {
  if (!client || !client->event_loop || !client->connection_watchdog_timer) {
    return;
  }

  // Remove existing timer (safe if not in event loop)
  event_del(client->connection_watchdog_timer);

  // Start connection watchdog (10 seconds timeout)
  struct timeval tv = {.tv_sec = 10, .tv_usec = 0};
  evtimer_add(client->connection_watchdog_timer, &tv);
}


/**
 * @brief Schedule reconnect timer - set delay before retrying connection
 *
 * @param client Redis client instance
 *
 * @note Uses exponential backoff strategy to prevent network congestion
 * @note Maximum delay limited to 30 seconds
 */
static void _configure_and_start_reconnect_timer(RedisClient *client,
                                                 int immediate) {
  if (!client || !client->reconnect_timer) {
    return;
  }

  // Remove existing timer (safe if not in event loop)
  event_del(client->reconnect_timer);
  // Calculate delay
  int delay_ms;
  if (immediate) {
    delay_ms = 0;
  } else {
    // Calculate delay (exponential backoff, max 30 seconds)
    delay_ms = client->reconnect_delay_ms;
    if (delay_ms > 30000) {
      delay_ms = 30000;
    }
    // Update reconnection parameters only for delayed retries
    client->reconnect_attempts++;
    client->reconnect_delay_ms *= 2; // Exponential backoff: double the delay
  }
  if (immediate) {
    printf("[INFO] Connection timer set up to execute immediately\n");
  } else {
    printf("[INFO] Waiting %dms before retry...\n", delay_ms);
  }
  fflush(stdout);

  // Set up timer
  struct timeval tv = {.tv_sec = delay_ms / 1000,
                       .tv_usec = (delay_ms % 1000) * 1000};
  evtimer_add(client->reconnect_timer, &tv);
}

/**
 * @brief Reconnect timer timeout callback - handles all state combinations
 *
 * @details This function implements the state machine logic for deciding when
 *          to initiate a new connection. It handles 6 possible combinations of
 *          (is_reconnecting, state).
 *
 * State machine dispatch table:
 *
 * | is_reconnecting | state        | Action                           |
 * |-----------------|--------------|----------------------------------|
 * | 0               | DISCONNECTED | Normal idle, initiate connection |
 * | 0               | CONNECTING   | Abnormal, force reconnect         |
 * | 0               | CONNECTED    | Normal, do nothing                |
 * | 1               | DISCONNECTED | Abnormal, force reconnect         |
 * | 1               | CONNECTING   | Normal timeout, force reconnect    |
 * | 1               | CONNECTED    | Abnormal, clear flag              |
 *
 * @param fd File descriptor (libevent parameter, unused)
 * @param events Event type (libevent parameter, unused)
 * @param arg User data (RedisClient pointer)
 */
static void _on_reconnect_timeout(evutil_socket_t fd, short events, void *arg) {
  RedisClient *client = (RedisClient *)arg;
  if (!client) return;

  REDIS_CLIENT_LOCK(client);
  int is_reconnecting = client->is_reconnecting;
  RedisClientState state = client->state;
  REDIS_CLIENT_UNLOCK(client);

  // ========================================================================
  // State Machine: All 6 combinations of (is_reconnecting, state)
  // ========================================================================

  // (0, RC_DISCONNECTED) - Idle state, initiate connection
  // Matrix row: DISCONNECTED + connect/reconnect → CONNECTING
  if (is_reconnecting == 0 && state == RC_DISCONNECTED) {
    _sm_disconnected_to_connecting(client);
    return;
  }

  // (0, RC_CONNECTING) - Abnormal: connecting but flag not set
  // Force reconnect to recover
  if (is_reconnecting == 0 && state == RC_CONNECTING) {
    printf("[WARN] Abnormal: connecting but flag not set, forcing reconnect\n");
    _sm_disconnected_to_connecting(client);
    return;
  }

  // (0, RC_CONNECTED) - Normal: connected, nothing to do
  if (is_reconnecting == 0 && state == RC_CONNECTED) {
    return;
  }

  // (1, RC_DISCONNECTED) - Abnormal: flag set but disconnected
  // Force reconnect to recover
  if (is_reconnecting == 1 && state == RC_DISCONNECTED) {
    printf("[WARN] Abnormal: reconnecting flag set but disconnected, forcing reconnect\n");
    _sm_disconnected_to_connecting(client);
    return;
  }

  // (1, RC_CONNECTING) - Normal: reconnecting timeout, force reconnect
  // Matrix row: CONNECTING + watchdog_timeout → DISCONNECTED (via watchdog)
  // Then: DISCONNECTED + reconnect → CONNECTING (here)
  if (is_reconnecting == 1 && state == RC_CONNECTING) {
    printf("[INFO] Reconnect timeout, initiating new connection\n");
    _sm_disconnected_to_connecting(client);
    return;
  }

  // (1, RC_CONNECTED) - Abnormal: flag set but connected, clear flag
  if (is_reconnecting == 1 && state == RC_CONNECTED) {
    printf("[WARN] Abnormal: reconnecting flag set but connected, clearing flag\n");
    REDIS_CLIENT_LOCK(client);
    client->is_reconnecting = 0;
    REDIS_CLIENT_UNLOCK(client);
    return;
  }
}

/**
 * @brief Actually initiate connection to Redis server
 */
// Note: _do_initiate_connection() functionality has been moved to
// _sm_disconnected_to_connecting() as part of state machine refactoring.
// This function is kept for reference only.

/**
 * @brief Initiate connection and start auto-reconnect mechanism
 *
 * @param client Redis client instance
 * @return 1 means success (connection initiated), 0 means failure
 *
 * @note If connection disconnects, will automatically attempt to reconnect
 * @note This function creates event loop (if doesn't exist)
 */
int redis_client_connect(RedisClient *client) {
  if (!client) {
    printf("redis_client_connect  !client   error!");
    return 0;
  }
  // Check if event loop is properly set up
  if (!client->event_loop) {
    // Create own event loop
    client->event_loop = event_base_new();
    if (!client->event_loop) {
      printf("[ERROR] Event loop creation failed\n");
      return 0;
    }
    printf("[INFO] Event loop created successfully\n");

    // Initialize all timers after event loop is created
    // This makes it clear what resources the client uses
    client->keepalive_timer = evtimer_new(client->event_loop, _on_keepalive_timeout, client);
    client->reconnect_timer = evtimer_new(client->event_loop, _on_reconnect_timeout, client);
    client->connection_watchdog_timer = evtimer_new(client->event_loop, _on_connection_watchdog_timeout, client);
    client->periodic_timer =  evtimer_new(client->event_loop, _on_periodic_timeout, client);
    printf("[INFO] All timers initialized: keepalive, reconnect, "
           "connection_watchdog, periodic\n");
    printf("[NOTE] Timers will be started when needed (connection_watchdog on "
           "connect, periodic on set_callbacks)\n");
  } else {
    // if event_loop exists, use it
    printf("[INFO] Using existing event loop\n");
  }

  // Start keepalive timer, ensure event loop stays active
  _configure_and_start_keepalive_timer(client);
  // Start connection immediately using reconnect timer
  _configure_and_start_reconnect_timer(client, 1);

  return 1;
}

/**
 * @brief Run event loop (blocking)
 *
 * @param client Redis client instance
 *
 * @note This function blocks current thread
 * @note Usually called in main thread
 */
void redis_client_run(RedisClient *client) {
  if (!client || !client->event_loop) {
    printf("redis_client_run  !client   error!");
    return;
  }

  const char *type = (client->client_type == RCT_PUBLISHER)    ? "PUB"
                     : (client->client_type == RCT_SUBSCRIBER) ? "SUB"
                                                               : "???";
  printf("[EVENTLOOP] %s client %p: loop starting\n", type, (void *)client);
  fflush(stdout);

  // Use loop with flags to prevent premature exit
  int result = event_base_loop(client->event_loop, EVLOOP_NO_EXIT_ON_EMPTY);

  if (result == 0) {
    printf("[EVENTLOOP] %s client %p: loop ended normally\n", type,
           (void *)client);
  } else if (result == 1) {
    printf("[EVENTLOOP] %s client %p: loop exited due to no events\n", type,
           (void *)client);
  } else {
    printf("[EVENTLOOP ERROR] %s client %p: loop failed with error %d\n", type,
           (void *)client, result);
  }

  fflush(stdout);
}

/**
 * @brief Send command to Redis server
 *
 * @param client Redis client instance
 * @param callback Callback function after command execution completes
 * @param privdata Private data passed to callback function
 * @param format Redis command format string (similar to printf)
 * @param ... Variable argument list, corresponding to parameters in format
 * string
 *
 * @note Command will be ignored if connection is not ready
 * @note Uses hiredis formatted command interface
 */
void redis_client_command(RedisClient *client, RedisCommandCallback callback,
                          void *privdata, const char *format, ...) {
  // Check if client is valid and connection status is normal
  if (!client || !client->redis_ctx || client->state != RC_CONNECTED) {
    printf(
        "[WARN] redis_client_command connection not ready, command ignored\n");
    return;
  }

  // Initialize variable argument list
  va_list args;
  va_start(args, format);

  char *cmd = NULL;
  // Use redisvFormatCommand to format command
  int len = redisvFormatCommand(&cmd, format, args);
  // Clean up variable argument list
  va_end(args);

  // Check if command formatting succeeded
  if (len < 0 || !cmd) {
    printf("[ERROR] Command formatting failed\n");
    return;
  }

  printf("[DEBUG] Sending command: %.*s\n", len, cmd);
  // Send formatted command to Redis server
  redisAsyncFormattedCommand(client->redis_ctx, callback, privdata, cmd, len);
  // Free memory occupied by formatted command
  free(cmd);
}