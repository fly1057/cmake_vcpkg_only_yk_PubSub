/**
 * @file redis_client.h
 * @brief Asynchronous Redis client based on hiredis + libevent with auto-reconnect
 *
 * Core features:
 * 1. Asynchronous connection to Redis server
 * 2. Automatic reconnection on disconnect (exponential backoff strategy)
 * 3. Support for user business callbacks (executed after connection is ready)
 * 4. Thread-safe event-driven model
 *
 * Usage flow:
 * 1. redis_client_create()     - Create client instance
 * 2. redis_client_set_callbacks() - Set callback functions
 * 3. redis_client_connect()    - Initiate connection (with auto-reconnect)
 * 4. redis_client_run()        - Run event loop
 * 5. redis_client_free()       - Free resources
 */

#pragma once
#ifndef REDIS_CLIENT_H
#define REDIS_CLIENT_H

#include <hiredis/async.h>
#include <hiredis/adapters/libevent.h>
#include <event2/event.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Type Definitions
// ============================================================================

/**
 * @brief Redis client connection state enumeration
 *
 * @note State transition flow:
 *       RC_DISCONNECTED → RC_CONNECTING → RC_CONNECTED
 *       RC_CONNECTED → RC_DISCONNECTED (triggers reconnect on disconnect)
 */
typedef enum {
	RC_DISCONNECTED,   /// Disconnected state: not connected or disconnected
	RC_CONNECTING,     /// Connecting: attempting to establish connection
	RC_CONNECTED       /// Connected: connection successful and ready for commands
} RedisClientState;

/**
 * @brief Redis client type enumeration
 */
typedef enum {
	RCT_UNKNOWN,       /// Unknown client type
	RCT_PUBLISHER,     /// Publisher client: sends commands to Redis
	RCT_SUBSCRIBER     /// Subscriber client: receives messages from Redis
} RedisClientType;

/**
 * @brief Connection callback function type
 * @param client Redis client instance
 */
typedef void (*RedisConnectCallback)(void* client);

/**
 * @brief Disconnect callback function type
 * @param client Redis client instance
 */
typedef void (*RedisDisconnectCallback)(void* client);

/**
 * @brief Command callback function type
 * @param ac Redis async context
 * @param reply Redis command reply (redisReply* type)
 * @param privdata User private data
 *
 * @note reply can be cast to redisReply* type
 * @note If reply is NULL, command execution failed
 */
typedef void (*RedisCommandCallback)(redisAsyncContext* ac, void* reply, void* privdata);

/**
 * @brief Periodic task callback function type
 * @param client Redis client instance
 *
 * @note This callback executes periodically at specified intervals
 * @note Checks connection status during execution, only runs when connected
 */
typedef void (*RedisPeriodicCallback)(void* client);

/**
 * @brief Main Redis client structure
 *
 * @note This structure contains core data for connection management, reconnection mechanism, and callback functions
 * @note Created using redis_client_create(), freed using redis_client_free()
 */
typedef struct RedisClient {
	// ========== Core Components ==========
	redisAsyncContext* redis_ctx;       /// Redis async context (hiredis)
	struct event_base* event_loop;     /// libevent event loop
	RedisClientState state;            /// Current connection state
	RedisClientType client_type;       /// Client type (publisher/subscriber/unknown)

	// ========== Connection Configuration ==========
	char* host;                        /// Redis server address (e.g., "127.0.0.1")
	int port;                          /// Redis server port (e.g., 6379)
	char* password;                    /// Authentication password (NULL for no auth)

	// ========== Reconnection Control ==========
	int reconnect_attempts;            /// Current reconnection attempt count
	int reconnect_delay_ms;            /// Current reconnection delay (milliseconds)
	struct event* reconnect_timer;     /// Reconnection timer event (delay between retry attempts)
	int is_reconnecting;              /// Flag indicating if connection/reconnection is in progress (0=no, 1=yes)
	struct event* connection_watchdog_timer; /// Connection watchdog timer (detects connection timeout)

	// ========== Event Loop Keepalive ==========
	struct event* keepalive_timer;      /// Keepalive timer to ensure event loop stays active

	// ========== Periodic Task Control ==========
	RedisPeriodicCallback on_periodic;       /// Periodic task callback function (optional)
	int periodic_interval_ms;                /// Periodic task execution interval (milliseconds)
	struct event* periodic_timer;            /// Periodic task timer event

	// ========== Application Callbacks (Key Extension Points) ==========
	RedisConnectCallback on_connected;      /// Connection success callback (optional)
	RedisConnectCallback on_ready;          /// Connection ready callback (required) - for executing user business code
	RedisDisconnectCallback on_disconnected;/// Disconnection callback (optional)

	// ========== User Data ==========
	void* user_data;                    /// User-defined data pointer

	// ========== Thread Safety ==========
	#ifdef _WIN32
	CRITICAL_SECTION cs;                /// Critical section for thread-safe access
	#else
	pthread_mutex_t mutex;               /// Mutex for thread-safe access
	#endif
} RedisClient;

// ============================================================================
// 核心 API
// ============================================================================

/**
 * @brief 创建 Redis 客户端实例
 *
 * @param host Redis 服务器地址（如 "127.0.0.1"）
 * @param port Redis 服务器端口（如 6379）
 * @param password 认证密码（NULL 表示无需认证）
 * @return RedisClient* 成功返回实例指针，失败返回 NULL
 *
 * @note 创建后需要调用 redis_client_connect() 启动连接
 * @note 使用后必须调用 redis_client_free() 释放资源
 */
RedisClient* redis_client_create(const char* host, int port, const char* password);

/**
 * @brief 释放 Redis 客户端实例及其所有资源
 *
 * @param client 要释放的客户端实例
 *
 * @note 安全特性：函数本身处理 NULL 参数
 * @note 清理顺序：定时器 → Redis连接 → 事件循环 → 字符串资源 → 内存
 */
void redis_client_free(RedisClient* client);

/**
 * @brief 发起连接并启动自动重连机制
 *
 * @param client Redis 客户端实例
 * @return int 1表示成功（连接已发起），0表示失败
 *
 * @note 如果连接断开，会自动尝试重连
 * @note 此函数会创建事件循环（如果不存在）
 * @note 使用指数退避策略：1s, 2s, 4s, ..., 最大30s
 */
int redis_client_connect(RedisClient* client);

/**
 * @brief 设置用户自定义回调函数
 *
 * @param client Redis 客户端实例
 * @param on_connected 连接成功回调（可选）
 * @param on_ready 连接就绪回调（必须）- 用于执行用户业务代码
 * @param on_disconnected 连接断开回调（可选）
 * @param on_periodic 周期任务回调（可选）
 * @param periodic_interval_ms 周期任务间隔（毫秒，0表示不启用）
 *
 * @note on_ready 回调会在连接成功并准备好接收命令时调用
 * @note 重连成功后，on_ready 会再次被调用以恢复业务状态
 */
void redis_client_set_callbacks(RedisClient* client,
								RedisConnectCallback on_connected,
								RedisConnectCallback on_ready,
								RedisDisconnectCallback on_disconnected,
								RedisPeriodicCallback on_periodic,
								int periodic_interval_ms);

/**
 * @brief 设置客户端类型
 *
 * @param client Redis 客户端实例
 * @param type 客户端类型（RCT_PUBLISHER/RCT_SUBSCRIBER/RCT_UNKNOWN）
 *
 * @note 建议在创建客户端后、连接前设置类型
 * @note 用于心跳日志中标识客户端角色
 */
void redis_client_set_type(RedisClient* client, RedisClientType type);

/**
 * @brief 检查客户端是否处于已连接状态
 *
 * @param client Redis 客户端实例
 * @return int 1表示已连接，0表示未连接
 *
 * @note 连接状态包括三个条件：
 *       1. 客户端实例有效
 *       2. 状态为 RC_CONNECTED
 *       3. Redis 异步上下文有效
 */
int redis_client_is_connected(RedisClient* client);

/**
 * @brief 运行事件循环（阻塞）
 *
 * @param client Redis 客户端实例
 *
 * @note 此函数会阻塞当前线程
 * @note 通常在主线程中调用
 */
void redis_client_run(RedisClient* client);

// ============================================================================
// 命令发送辅助函数
// ============================================================================

/**
 * @brief 发送命令到 Redis 服务器
 *
 * @param client Redis 客户端实例
 * @param callback 命令执行完成后的回调函数
 * @param privdata 传递给回调函数的私有数据
 * @param format Redis 命令格式字符串（类似于 printf）
 * @param ... 可变参数列表，对应格式字符串中的参数
 *
 * @note 如果连接未就绪，命令会被忽略
 * @note 使用 hiredis 的格式化命令接口
 *
 * @example 发送 GET 命令
 * @code
 * redis_client_command(client, callback, NULL, "GET mykey");
 * @endcode
 *
 * @example 发送 SET 命令
 * @code
 * redis_client_command(client, callback, NULL, "SET %s %s", key, value);
 * @endcode
 *
 * @example 发送 HSET 命令
 * @code
 * redis_client_command(client, callback, NULL, "HSET user:%d name %s age %d", id, name, age);
 * @endcode
 */
void redis_client_command(RedisClient* client,
						 RedisCommandCallback callback,
						 void* privdata,
						 const char* format, ...);

#ifdef __cplusplus
}
#endif

#endif // REDIS_CLIENT_H
