/**
 * @file only_yk_PubSub.c
 * @brief YK command periodic publisher using asynchronous Redis client
 * 
 * Features:
 * 1. Asynchronous Redis client with auto-reconnection
 * 2. Periodic YK command publishing to "rc:scada:yk" channel
 * 3. Event-driven architecture using libevent
 * 4. Simple C implementation using existing redis_client and redis_client_data interfaces
 */

#define _GNU_SOURCE  /* Enable GNU extensions for strnlen and other functions */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>

#ifdef _WIN32
/* IMPORTANT: winsock2.h must be included before windows.h */
#include <winsock2.h>
#include <windows.h>
#else
#include <unistd.h>
#include <pthread.h>
#endif

/* Include Redis client headers */
#include "redis_client.h"
#include "redis_client_data.h"

/* Global flag for graceful shutdown */
static volatile int g_running = 1;

/* YK command counter for generating different commands */
static int g_yk_command_counter = 0;

/* Redis client instances - one for publish, one for subscribe */
static RedisClient* g_pub_client = NULL;
static RedisClient* g_sub_client = NULL;

/* ============================================================================
 * Callback Functions
 * ============================================================================ */
/**
 * @brief YK message received callback - called when YK command is received via subscription
 * @param channel Channel name
 * @param yk_data YK command data
 * @param user_data User data pointer
 */
static void on_yk_message_received(const char* channel, const REDIS_YK* yk_data, void* user_data) {
    // This callback is redundant - logs are already printed in _on_yk_message_received
    // No additional logging needed
    (void)channel;
    (void)yk_data;
    (void)user_data;
}

/**
 * @brief Callback when PUBLISH client connection is established
 * @param client Redis client instance
 */
static void on_pub_connected(void* client) {
    printf("[OK] PUB connected\n");
}

static void on_sub_connected(void* client) {
    printf("[OK] SUB connected\n");
}

static void on_pub_ready(void* client) {
    printf("[OK] PUB ready\n");
}

static void on_sub_ready(void* client) {
    const char* yk_channel = "rc:scada:yk";
    printf("[OK] SUB ready, (re)subscribing to %s\n", yk_channel);
    fflush(stdout);
    data_collection_subscribe_yk((RedisClient*)client, yk_channel, on_yk_message_received, NULL);
    printf("[OK] SUB subscribed to %s\n", yk_channel);
    fflush(stdout);
}

static void on_pub_disconnected(void* client) {
    printf("[WARN] PUB disconnected\n");
}

static void on_sub_disconnected(void* client) {
    printf("[WARN] SUB disconnected\n");
}

/**
 * @brief Callback when YK command is published successfully
 * @param channel Channel name (rc:scada:yk)
 * @param user_data User data pointer
 */
static void on_yk_published(const char* channel, void* user_data) {
    // This callback is redundant - logs are already printed in _on_yk_command_published
    // No additional logging needed
    (void)channel;
    (void)user_data;
}

/**
 * @brief Periodic task callback - publishes YK command periodically
 * @param client Redis client instance (unused - we use global pub client)
 */
static void periodic_yk_publish_task(void* client) {
    if (!g_pub_client || !redis_client_is_connected(g_pub_client)) {
        return;
    }

    /* Create YK command with varying parameters */
    REDIS_YK yk_cmd;
    memset(&yk_cmd, 0, sizeof(REDIS_YK));

    /* Set command parameters */
    yk_cmd.deviceId = 1;                          /* Device ID */
    yk_cmd.dataId = (g_yk_command_counter % 10) + 1;  /* Data ID (1-10) */
    yk_cmd.operaterType = (g_yk_command_counter % 2) + 1;  /* Operation type: 1=start, 2=stop */
    yk_cmd.val = 100 + g_yk_command_counter;      /* Command value */
    yk_cmd.status = 0;                            /* Status */
    yk_cmd.timestamp = 0;                         /* Timestamp will be set by data_collection_publish_yk */

    /* Publish YK command to rc:scada:yk channel using PUB client */
    data_collection_publish_yk(g_pub_client, &yk_cmd, on_yk_published, NULL);

    /* Increment command counter */
    g_yk_command_counter++;
}

/* ============================================================================
 * Signal Handlers
 * ============================================================================ */

#ifdef _WIN32
/**
 * @brief Windows signal handler for graceful shutdown
 */
BOOL WINAPI console_handler(DWORD signal) {
    if (signal == CTRL_C_EVENT || signal == CTRL_CLOSE_EVENT) {
        printf("\n[INFO] Received shutdown signal, stopping application...\n");
        fflush(stdout);
        g_running = 0;
        return TRUE;
    }
    return FALSE;
}
#else
/**
 * @brief Unix signal handler for graceful shutdown
 */
void signal_handler(int signal) {
    printf("\n[INFO] Received signal %d, stopping application...\n", signal);
    fflush(stdout);
    g_running = 0;
}
#endif

/* ============================================================================
 * Main Function
 * ============================================================================ */

int main(int argc, char* argv[]) {
    int result = 0;
    
    /* Configuration */
    const char* redis_host = "127.0.0.1";
    int redis_port = 6379;
    const char* redis_password = NULL;  /* Set to NULL if no password */
    int publish_interval_ms = 3000;      /* Publish every 3 seconds */
    
  
    /* Print simple banner */
    printf("========================================\n");
    printf("     YK Pub/Sub (Dual Client)\n");
    printf("========================================\n");
    printf("Host: %s, Port: %d, Interval: %dms\n",
           redis_host, redis_port, publish_interval_ms);
    printf("========================================\n");
    fflush(stdout);

    /* Setup signal handlers */
#ifdef _WIN32
    SetConsoleCtrlHandler(console_handler, TRUE);
#else
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
#endif

    /* Initialize Winsock on Windows (required for libevent) */
#ifdef _WIN32
    {
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            printf("[ERR] WSAStartup failed\n");
            result = 1;
            goto cleanup;
        }
    }
#endif

    /* Create clients */
    g_pub_client = redis_client_create(redis_host, redis_port, redis_password);
    if (!g_pub_client) {
        printf("[ERR] Failed to create PUB client\n");
        result = 1;
        goto cleanup;
    }
    redis_client_set_type(g_pub_client, RCT_PUBLISHER);
    printf("[OK] PUB client created\n");

    g_sub_client = redis_client_create(redis_host, redis_port, redis_password);
    if (!g_sub_client) {
        printf("[ERR] Failed to create SUB client\n");
        result = 1;
        goto cleanup;
    }
    redis_client_set_type(g_sub_client, RCT_SUBSCRIBER);
    printf("[OK] SUB client created\n");

    /* Set callbacks */
    redis_client_set_callbacks(g_pub_client, on_pub_connected, on_pub_ready, on_pub_disconnected, periodic_yk_publish_task, publish_interval_ms);
    redis_client_set_callbacks(g_sub_client, on_sub_connected, on_sub_ready, on_sub_disconnected, NULL, 0);

    /* Start connections with staggered timing (event loop created in connect) */
    if (!redis_client_connect(g_pub_client)) {
        printf("[ERR] Failed to start PUB client\n");
        result = 1;
        goto cleanup;
    }

#ifdef _WIN32
    Sleep(100);
#else
    usleep(100000);
#endif

    if (!redis_client_connect(g_sub_client)) {
        printf("[ERR] Failed to start SUB client\n");
        result = 1;
        goto cleanup;
    }

    printf("[OK] Clients started - PUB publishing, SUB listening\n");

    /* Start both event loops in separate threads */
#ifdef _WIN32
    HANDLE pub_thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)redis_client_run, g_pub_client, 0, NULL);
    HANDLE sub_thread = CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)redis_client_run, g_sub_client, 0, NULL);

    if (pub_thread == NULL || sub_thread == NULL) {
        printf("[ERR] Failed to create threads\n");
        result = 1;
        goto cleanup;
    }
#else
    pthread_t pub_thread, sub_thread;
    if (pthread_create(&pub_thread, NULL, (void*(*)(void*))redis_client_run, g_pub_client) != 0 ||
        pthread_create(&sub_thread, NULL, (void*(*)(void*))redis_client_run, g_sub_client) != 0) {
        printf("[ERR] Failed to create threads\n");
        result = 1;
        goto cleanup;
    }
#endif

    /* Wait for both clients to connect */
    printf("[INFO] Waiting for connections...\n");

    int max_wait = 10;
    int wait_count = 0;
    int pub_connected = 0;
    int sub_connected = 0;

    while (wait_count < max_wait && g_running) {
#ifdef _WIN32
        Sleep(500);
#else
        usleep(500000);
#endif

        if (!pub_connected && redis_client_is_connected(g_pub_client)) {
            printf("[OK] PUB connected\n");
            pub_connected = 1;
        }

        if (!sub_connected && redis_client_is_connected(g_sub_client)) {
            printf("[OK] SUB connected\n");
            sub_connected = 1;
        }

        wait_count++;
        
        if (pub_connected && sub_connected) {
            printf("[INFO] Both clients are connected, starting main loop\n");
            break;
        }
    }
    
    if (!pub_connected) {
        printf("[WARN] PUB client failed to connect within timeout\n");
    }
    
    if (!sub_connected) {
        printf("[ERROR] SUB client failed to connect within timeout - this is the problem!\n");
        printf("[ERROR] SUB client event loop may have exited prematurely\n");
    }
    
    /* Main thread will wait for both client threads to complete */
    printf("[INFO] Main thread waiting for client threads to complete...\n");
    printf("[INFO] Press Ctrl+C to exit gracefully\n");
    fflush(stdout);
    
    /* Wait for graceful shutdown signal */
    while (g_running) {
#ifdef _WIN32
        Sleep(1000);
#else
        sleep(1);
#endif
    }
    
    /* Graceful shutdown: stop event loops and wait for threads to complete */
    printf("[INFO] Graceful shutdown initiated...\n");
    fflush(stdout);
    
    /* Stop event loops */
    if (g_pub_client && g_pub_client->event_loop) {
        event_base_loopbreak(g_pub_client->event_loop);
    }

    if (g_sub_client && g_sub_client->event_loop) {
        event_base_loopbreak(g_sub_client->event_loop);
    }

    /* Wait for threads */
#ifdef _WIN32
    WaitForMultipleObjects(2, (HANDLE[]){pub_thread, sub_thread}, FALSE, 5000);
    CloseHandle(pub_thread);
    CloseHandle(sub_thread);
#else
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 5;
    pthread_timedjoin_np(pub_thread, NULL, &ts);
    pthread_timedjoin_np(sub_thread, NULL, &ts);
#endif

cleanup:
	printf("[INFO] Cleanup...\n");

	if (g_pub_client) {
		redis_client_free(g_pub_client);
		g_pub_client = NULL;
	}

	if (g_sub_client) {
		redis_client_free(g_sub_client);
		g_sub_client = NULL;
	}
    

    
    printf("[INFO] Application shutdown complete (result=%d)\n", result);
    fflush(stdout);
    
    return result;
}

/**
 * @brief Usage information
 */
void print_usage(const char* program_name) {
    printf("Usage: %s [host] [port] [password] [interval_ms]\n", program_name);
    printf("\nArguments:\n");
    printf("  host         Redis server host (default: 127.0.0.1)\n");
    printf("  port         Redis server port (default: 6379)\n");
    printf("  password     Redis password (default: none)\n");
    printf("  interval_ms  Publish interval in milliseconds (default: 3000)\n");
    printf("\nExample:\n");
    printf("  %s 192.168.1.100 6379 mypassword 5000\n", program_name);
    printf("  %s 127.0.0.1 6379 \"\" 1000\n", program_name);
    fflush(stdout);
}
