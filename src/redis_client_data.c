/**
 * @file redis_client_data.c
 * @brief Data collection management implementation
 */

#include "redis_client_data.h"
#include "redis_client.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Internal function declarations */
static void _make_redis_key(char* key, int station_id, int point_id, DataType type);  /* Generate Redis key */
static void _on_data_item_loaded(redisAsyncContext* ac, void* reply, void* privdata);  /* Single data item load callback */
static void _on_yk_command_published(redisAsyncContext* ac, void* reply, void* privdata);  /* Publish YK command callback */
static void _on_yt_command_published(redisAsyncContext* ac, void* reply, void* privdata);  /* Publish YT command callback */
static void _on_yk_message_received(redisAsyncContext* ac, void* reply, void* privdata);  /* Subscribe YK command callback */
static void _on_yt_message_received(redisAsyncContext* ac, void* reply, void* privdata);  /* Subscribe YT command callback */

/* Get current timestamp in milliseconds */
static long long get_current_timestamp(void) {
    struct timeval tv;
    evutil_gettimeofday(&tv, NULL);
    return (long long)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

/* Find data item by key (e.g., yc:1:3) */
DataItem* find_item_by_key(DataCollection* collection, DataType type, int station_id, int point_id) {
    /* Traverse to find */
    for (int i = 0; i < collection->count; i++) {
        DataItem* item = &(collection->items[i]);
        if (item->type == type && item->station_id == station_id && item->point_id == point_id) {
            return item;
        }
    }
    return NULL;
}

/* Publish YK command context (temporary, one-time use) */
typedef struct {
    char channel[64];                  /* Channel name */
    REDIS_YK yk_cmd;                  /* YK command data */
    PublishCommandCallback callback;   /* User callback */
    void* user_data;                   /* User data */
} PublishYKContext;

/* Publish YT command context (temporary, one-time use) */
typedef struct {
    char channel[64];                  /* Channel name */
    REDIS_YT yt_cmd;                  /* YT command data */
    PublishCommandCallback callback;   /* User callback */
    void* user_data;                   /* User data */
} PublishYTContext;

/* Subscribe YK command context (temporary, one-time use) */
typedef struct {
    char channel[64];                  /* Channel name */
    SubscribeMessageCallback callback; /* User callback */
    void* user_data;                   /* User data */
} SubscribeYKContext;

/* Subscribe YT command context (temporary, one-time use) */
typedef struct {
    char channel[64];                  /* Channel name */
    SubscribeYTMessageCallback callback; /* User callback */
    void* user_data;                   /* User data */
} SubscribeYTContext;

/* Create data collection */
DataCollection* data_collection_create(void) {
    DataCollection* coll = (DataCollection*)malloc(sizeof(DataCollection));
    if (!coll) {
        printf("[ERROR] Failed to allocate memory for DataCollection\n");
        return NULL;
    }
    memset(coll, 0, sizeof(DataCollection));
    return coll;
}

/* Free data collection */
void data_collection_free(DataCollection* collection) {
    if (collection) {
        free(collection);
    }
}

/* Add data item to collection */
int data_collection_add(DataCollection* collection,
                        int station_id,
                        int point_id,
                        DataType type,
                        int is_valid) {
    if (!collection || collection->count >= 2048) return 0;

    DataItem* item = &collection->items[collection->count]; // Add to the end
    item->station_id = station_id;
    item->point_id = point_id;
    item->type = type;
    item->is_valid = is_valid;
    item->value = 0.0;
    item->int_value = 0;
    item->is_loaded = 0;
    collection->count++;
    return 1;
}

/* Async load all data items from Redis */
void data_collection_load_from_redis(DataCollection* collection,struct RedisClient* client) {
    if (!collection || !client) return;

    /* Reset loading count */
    collection->loaded_count = 0;

    /* Traverse all data items, send async GET commands */
    for (int i = 0; i < collection->count; i++) {
        DataItem* item = &(collection->items[i]);

        /* Create request context, used to find corresponding data item in callback */
        RedisRequestContext* req_ctx = (RedisRequestContext*)malloc(sizeof(RedisRequestContext));
        req_ctx->collection = collection;
        req_ctx->item_index = i;
        _make_redis_key(req_ctx->key, item->station_id, item->point_id, item->type);

        /* Send async GET command with context */
        redisAsyncCommand(client->redis_ctx, _on_data_item_loaded, req_ctx, "HMGET  %s realValue", req_ctx->key);
    }
}

/* Generate Redis key name, format: yc:1:1, yx:2:3, etc. */
static void _make_redis_key(char* key, int station_id, int point_id, DataType type) {
    const char* type_str;
    switch (type) {
        case DATA_TYPE_YC: type_str = "yc"; break;  /* Telemetry */
        case DATA_TYPE_YX: type_str = "yx"; break;  /* Remote signal */
        case DATA_TYPE_YT: type_str = "yt"; break;  /* Energy */
        case DATA_TYPE_YK: type_str = "yk"; break;  /* Remote control */
        default: type_str = "unknown"; break;
    }
    snprintf(key, 32, "%s:%d:%d", type_str, station_id, point_id);
}

/* Single data item loading complete callback function */
static void _on_data_item_loaded(redisAsyncContext* ac, void* reply, void* privdata) {
    /* Get request context from private data */
    RedisRequestContext* req_ctx = (RedisRequestContext*)privdata;
    
    /* 安全检查：确保上下文和数据集合有效 */
    if (!req_ctx || !req_ctx->collection) {
        printf("[ERROR] Invalid request context in data load callback\n");
        if (req_ctx) free(req_ctx);
        return;
    }
    DataItem* item = &(req_ctx->collection->items[req_ctx->item_index]);  /* Find corresponding data item by index */
    redisReply* r = (redisReply*)reply;

    /* Process response - HMGET returns array type */
    if (r && r->type == REDIS_REPLY_ARRAY && r->elements > 0) {
        /* Get first element of array (realValue field) */
        redisReply* field_reply = r->element[0];
        if (field_reply && field_reply->type == REDIS_REPLY_STRING
            && field_reply->len == sizeof(double)) {
            /* YC data, binary double format: read directly from byte stream */
            item->value = *((double*)field_reply->str);
            item->int_value = (int)item->value;
            item->is_loaded = 1;
            req_ctx->collection->loaded_count++;
            printf("[LOAD] %s.realValue = %f (binary, loaded %d/%d)\n",
                   req_ctx->key, item->value, req_ctx->collection->loaded_count, req_ctx->collection->count);
        }
        else if (field_reply && field_reply->type == REDIS_REPLY_STRING
            && field_reply->len == sizeof(short)) {
            /* YX data, binary short format: read directly from byte stream */
            item->value = *((short*)field_reply->str);
            item->int_value = (short)item->value;
            item->is_loaded = 1;
            req_ctx->collection->loaded_count++;
            printf("[LOAD] %s.realValue = %d (binary short, loaded %d/%d)\n",
                   req_ctx->key, item->int_value, req_ctx->collection->loaded_count, req_ctx->collection->count);
        }
        else if (field_reply && field_reply->type == REDIS_REPLY_STRING) {
            /* String format compatibility: parse value */
            item->value = atof(field_reply->str);
            item->int_value = atoi(field_reply->str);
            item->is_loaded = 1;
            req_ctx->collection->loaded_count++;
            printf("[LOAD] %s.realValue = %s (string, loaded %d/%d)\n",
                   req_ctx->key, field_reply->str, req_ctx->collection->loaded_count, req_ctx->collection->count);
        } else {
            /* Field value is NIL or not exist, mark as loaded but no value */
            item->is_loaded = 1;
            req_ctx->collection->loaded_count++;
            printf("[LOAD] %s.realValue = NULL (loaded %d/%d)\n",
                   req_ctx->key, req_ctx->collection->loaded_count, req_ctx->collection->count);
        }
    } else if (r && r->type == REDIS_REPLY_NIL) {
        /* Key not exist, mark as loaded but no value */
        item->is_loaded = 1;
        req_ctx->collection->loaded_count++;
        printf("[LOAD] %s = NULL (loaded %d/%d)\n",
               req_ctx->key, req_ctx->collection->loaded_count, req_ctx->collection->count);
    } else {
        /* Load failed */
        printf("[ERROR] Failed to load %s (reply type: %d)\n", req_ctx->key, r ? r->type : -1);
    }

    /* Check if all data items are loaded */
    if (req_ctx->collection->loaded_count >= req_ctx->collection->count) {
        if (req_ctx->collection->on_all_data_loaded) {
            /* Call user callback */
            req_ctx->collection->on_all_data_loaded(req_ctx->collection->user_data);
        }
    }

    /* Release request context */
    free(req_ctx);
}

/* Publish YK command callback function */
static void _on_yk_command_published(redisAsyncContext* ac, void* reply, void* privdata) {
    PublishYKContext* config_ctx = (PublishYKContext*)privdata;

    /* Check if async context is valid */
    if (!ac) {
        printf("[ERR] YK publish: async context is NULL\n");
        if (config_ctx) free(config_ctx);
        return;
    }

    /* Check client association */
    RedisClient* client = (RedisClient*)ac->data;
    const char* type_str = client ? ((client->client_type == RCT_PUBLISHER) ? "PUB" : (client->client_type == RCT_SUBSCRIBER) ? "SUB" : "???") : "NULL";

    /* 安全检查：确保配置上下文有效 */
    if (!config_ctx) {
        printf("[ERR] YK publish: invalid context (ac=%p, client_type=%s)\n", (void*)ac, type_str);
        return;
    }
    redisReply* r = (redisReply*)reply;

    if (r && r->type == REDIS_REPLY_INTEGER) {
        printf("[PUB] YK: dev=%d, id=%d, type=%d, val=%d, ts=%lld, subs=%lld\n",
               config_ctx->yk_cmd.deviceId, config_ctx->yk_cmd.dataId,
               config_ctx->yk_cmd.operaterType, config_ctx->yk_cmd.val,
               config_ctx->yk_cmd.timestamp, r->integer);
    } else {
        printf("[ERR] YK publish failed: dev=%d, id=%d, reply_type=%d, ac=%p\n",
               config_ctx->yk_cmd.deviceId, config_ctx->yk_cmd.dataId,
               r ? r->type : -1, (void*)ac);
        if (r && r->type == REDIS_REPLY_ERROR) {
            printf("[ERR] Redis error: %s\n", r->str ? r->str : "unknown");
        }
    }

    /* Call user callback */
    if (config_ctx->callback) {
        config_ctx->callback(config_ctx->channel, &config_ctx->yk_cmd);
    }

    /* Release config context */
    free(config_ctx);
}

/* Publish YT command callback function */
static void _on_yt_command_published(redisAsyncContext* ac, void* reply, void* privdata) {
    PublishYTContext* config_ctx = (PublishYTContext*)privdata;

    /* 安全检查：确保配置上下文有效 */
    if (!config_ctx) {
        printf("[ERR] YT publish: invalid context\n");
        return;
    }
    redisReply* r = (redisReply*)reply;

    if (r && r->type == REDIS_REPLY_INTEGER) {
        printf("[PUB] YT: dev=%d, id=%d, val=%.2f, ts=%lld, subs=%lld\n",
               config_ctx->yt_cmd.deviceId, config_ctx->yt_cmd.dataId,
               config_ctx->yt_cmd.val, config_ctx->yt_cmd.timestamp, r->integer);
    } else {
        printf("[ERR] YT publish failed: dev=%d, id=%d, reply_type=%d\n",
               config_ctx->yt_cmd.deviceId, config_ctx->yt_cmd.dataId,
               r ? r->type : -1);
        if (r && r->type == REDIS_REPLY_ERROR) {
            printf("[ERR] Redis error: %s\n", r->str ? r->str : "unknown");
        }
    }

    /* Call user callback */
    if (config_ctx->callback) {
        config_ctx->callback(config_ctx->channel, &config_ctx->yt_cmd);
    }

    /* Release config context */
    free(config_ctx);
}

/* Subscribe YK command callback function */
static void _on_yk_message_received(redisAsyncContext* ac, void* reply, void* privdata) {
    SubscribeYKContext* config_ctx = (SubscribeYKContext*)privdata;

    /* Check if async context is valid */
    if (!ac) {
        printf("[ERR] YK subscribe: async context is NULL\n");
        return;
    }

    /* Check if the connection is still associated with a client */
    RedisClient* client = (RedisClient*)ac->data;
    if (!client) {
        printf("[ERR] YK subscribe: client association lost (ac=%p, ac->data=NULL)\n", (void*)ac);
        return;
    }

    /* 安全检查：确保配置上下文有效 */
    if (!config_ctx) {
        printf("[ERR] YK subscribe: invalid context\n");
        return;
    }

    redisReply* r = (redisReply*)reply;

    /* Check if reply is valid */
    if (!r) {
        printf("[ERR] YK subscribe: reply is NULL (channel=%s, ac=%p)\n", config_ctx->channel, (void*)ac);
        return;
    }

    /* Check if reply is an error */
    if (r->type == REDIS_REPLY_ERROR) {
        printf("[ERR] YK subscribe: Redis error - %s (channel=%s)\n", 
               r->str ? r->str : "unknown", config_ctx->channel);
        return;
    }

    if (r && r->type == REDIS_REPLY_ARRAY && r->elements == 3) {
        /* PUB/SUB message format: [message, channel, payload] or [subscribe, channel, num_subscribers] */
        const char* type = r->element[0]->str;

        if (strcmp(type, "message") == 0) {
            /* 处理接收到的消息 */
            redisReply* payload = r->element[2];

            if (payload->type == REDIS_REPLY_STRING && payload->len == sizeof(REDIS_YK)) {
                REDIS_YK* yk_data = (REDIS_YK*)payload->str;
                printf("[SUB] YK: dev=%d, id=%d, type=%d, val=%d, ts=%lld\n",
                       yk_data->deviceId, yk_data->dataId,
                       yk_data->operaterType, yk_data->val, yk_data->timestamp);

                /* Call user callback */
                if (config_ctx->callback) {
                    config_ctx->callback(config_ctx->channel, yk_data, config_ctx->user_data);
                } else {
                    printf("[ERR] YK callback is NULL\n");
                }
            } else {
                printf("[ERR] YK payload invalid: type=%d, len=%zu\n", payload->type, payload->len);
            }
        }
        else if (strcmp(type, "subscribe") == 0) {
            /* 订阅成功确认消息 */
            const char* channel = r->element[1]->str;
            long num_subscribers = r->element[2]->integer;
            printf("[SUB] YK: subscribed to %s (subs=%ld)\n", channel, num_subscribers);
        }
    }
    /* 注意：不释放订阅配置上下文，因为它需要持续接收消息 */
}

/* Subscribe YT command callback function */
static void _on_yt_message_received(redisAsyncContext* ac, void* reply, void* privdata) {
    SubscribeYTContext* config_ctx = (SubscribeYTContext*)privdata;

    /* 安全检查：确保配置上下文有效 */
    if (!config_ctx) {
        printf("[ERR] YT subscribe: invalid context\n");
        return;
    }

    redisReply* r = (redisReply*)reply;

    if (r && r->type == REDIS_REPLY_ARRAY && r->elements == 3) {
        /* PUB/SUB message format: [message, channel, payload] or [subscribe, channel, num_subscribers] */
        const char* type = r->element[0]->str;

        if (strcmp(type, "message") == 0) {
            /* 处理接收到的消息 */
            redisReply* payload = r->element[2];

            if (payload->type == REDIS_REPLY_STRING && payload->len == sizeof(REDIS_YT)) {
                REDIS_YT* yt_data = (REDIS_YT*)payload->str;
                printf("[SUB] YT: dev=%d, id=%d, val=%.2f, ts=%lld\n",
                       yt_data->deviceId, yt_data->dataId,
                       yt_data->val, yt_data->timestamp);

                /* Call user callback */
                if (config_ctx->callback) {
                    config_ctx->callback(config_ctx->channel, yt_data, config_ctx->user_data);
                } else {
                    printf("[ERR] YT callback is NULL\n");
                }
            } else {
                printf("[ERR] YT payload invalid: type=%d, len=%zu\n", payload->type, payload->len);
            }
        }
        else if (strcmp(type, "subscribe") == 0) {
            /* 订阅成功确认消息 */
            const char* channel = r->element[1]->str;
            long num_subscribers = r->element[2]->integer;
            printf("[SUB] YT: subscribed to %s (subs=%ld)\n", channel, num_subscribers);
        }
    }
    /* 注意：不释放订阅配置上下文，因为它需要持续接收消息 */
}

/* Publish YK to Redis (PUB/SUB, with callback) */
void data_collection_publish_yk(struct RedisClient* client, const REDIS_YK* yk_cmd,
                               PublishCommandCallback callback, void* user_data) {
    if (!client || !client->redis_ctx || !yk_cmd)
    {
        printf("data_collection_publish_yk   !client || !client->redis_ctx || !yk_cmd  error!  ");
        return;
    }
    
    /* Log which client is publishing */
    const char* type_str = (client->client_type == RCT_PUBLISHER) ? "PUB" : (client->client_type == RCT_SUBSCRIBER) ? "SUB" : "???";
    printf("[PUB-CMD] %s client %p: publishing YK (dev=%d, id=%d), redis_ctx=%p\n", 
           type_str, (void*)client, yk_cmd->deviceId, yk_cmd->dataId, (void*)client->redis_ctx);
    fflush(stdout);
    
    /* Create publish config context */
    PublishYKContext* config_ctx = (PublishYKContext*)malloc(sizeof(PublishYKContext));
    if (!config_ctx) return;
    
    /* 完全初始化上下文结构 */
    memset(config_ctx, 0, sizeof(PublishYKContext));

    /* Build channel name */
    snprintf(config_ctx->channel, sizeof(config_ctx->channel), "rc:scada:yk");

    /* Copy YK command data to context */
    memcpy(&config_ctx->yk_cmd, yk_cmd, sizeof(REDIS_YK));

    config_ctx->callback = callback;
    config_ctx->user_data = user_data;

    /* 在发布前设置时间戳 */
    config_ctx->yk_cmd.timestamp = get_current_timestamp();  /* 获取当前时间 */
    
    /* 发布带时间戳的命令 */
    redisAsyncCommand(client->redis_ctx, _on_yk_command_published, config_ctx, 
                     "PUBLISH %s %b", config_ctx->channel, &config_ctx->yk_cmd, sizeof(REDIS_YK));
}

/* Subscribe to YK command channel */
void data_collection_subscribe_yk(struct RedisClient* client, const char* channel, SubscribeMessageCallback callback, void* user_data) {
	if (!client) {
		printf("[ERROR] data_collection_subscribe_yk: client is NULL\n");
		return;
	}
	if (!client->redis_ctx) {
		printf("[ERROR] data_collection_subscribe_yk: redis_ctx is NULL (client=%p)\n", (void*)client);
		return;
	}

	// Check if connection is actually connected
	if (client->state != RC_CONNECTED) {
		printf("[ERROR] data_collection_subscribe_yk: client not connected, state=%d (client=%p)\n", client->state, (void*)client);
		return;
	}

    /* Create subscribe config context */
    SubscribeYKContext* config_ctx = (SubscribeYKContext*)malloc(sizeof(SubscribeYKContext));
    if (!config_ctx) {
        printf("[ERROR] Failed to allocate YK subscribe context\n");
        return;
    }

    /* 完全初始化上下文结构 */
    memset(config_ctx, 0, sizeof(SubscribeYKContext));

    snprintf(config_ctx->channel, sizeof(config_ctx->channel), "%s", channel);
    config_ctx->callback = callback;
    config_ctx->user_data = user_data;

    /* Use SUBSCRIBE (not PSUBSCRIBE) for exact channel match */
    const char* type = (client->client_type == RCT_PUBLISHER) ? "PUB" : (client->client_type == RCT_SUBSCRIBER) ? "SUB" : "???";
    printf("[SUBSCRIBE] %s client %p: sending SUBSCRIBE %s command, redis_ctx=%p\n", type, (void*)client, channel, (void*)client->redis_ctx);
    fflush(stdout);
    redisAsyncCommand(client->redis_ctx, _on_yk_message_received, config_ctx, "SUBSCRIBE %s", channel);
}

/* Publish YT to Redis (PUB/SUB, with callback) */
void data_collection_publish_yt(struct RedisClient* client, const REDIS_YT* yt_cmd, 
                               PublishCommandCallback callback, void* user_data) {
	if (!client || !client->redis_ctx || !yt_cmd)
	{
		printf("data_collection_publish_yt   !client || !client->redis_ctx || !yt_cmd  error!  ");
		return;
	}

    /* Create publish context */
    PublishYTContext* ctx = (PublishYTContext*)malloc(sizeof(PublishYTContext));
    if (!ctx) return;
    
    /* 完全初始化上下文结构 */
    memset(ctx, 0, sizeof(PublishYTContext));

    /* Build channel name */
    snprintf(ctx->channel, sizeof(ctx->channel), "rc:scada:yt");

    /* Copy YT command data to context */
    memcpy(&ctx->yt_cmd, yt_cmd, sizeof(REDIS_YT));

    ctx->callback = callback;
    ctx->user_data = user_data;

    /* 在发布前设置时间戳 */
    ctx->yt_cmd.timestamp = get_current_timestamp();  /* 获取当前时间 */
    
    /* 发布带时间戳的命令 */
    redisAsyncCommand(client->redis_ctx, _on_yt_command_published, ctx, 
                     "PUBLISH %s %b", ctx->channel, &ctx->yt_cmd, sizeof(REDIS_YT));
}

/* Subscribe to YT command channel */
void data_collection_subscribe_yt(struct RedisClient* client, const char* channel, SubscribeYTMessageCallback callback, void* user_data) {
	if (!client || !client->redis_ctx) {
		printf("data_collection_subscribe_yt   !client || !client->redis_ctx || !yk_cmd  error!  ");
		return;
	}


    /* Create subscribe config context */
    SubscribeYTContext* config_ctx = (SubscribeYTContext*)malloc(sizeof(SubscribeYTContext));
    if (!config_ctx) {
        printf("[ERROR] Failed to allocate YT subscribe context\n");
        return;
    }

    /* 完全初始化上下文结构 */
    memset(config_ctx, 0, sizeof(SubscribeYTContext));
    
    snprintf(config_ctx->channel, sizeof(config_ctx->channel), "%s", channel);
    config_ctx->callback = callback;
    config_ctx->user_data = user_data;
    
    /* Use SUBSCRIBE (not PSUBSCRIBE) for exact channel match */
    redisAsyncCommand(client->redis_ctx, _on_yt_message_received, config_ctx, "SUBSCRIBE %s", channel);
}

/* Publish scheduled YK commands */
void data_collection_publish_scheduled_yk(struct RedisClient* client, const REDIS_YK* commands, int count, PublishCommandCallback callback, void* user_data) {
	if (!client || !client->redis_ctx || !commands)		{
        printf("[ERR] YK scheduled: invalid params\n");
	return;
}
    int valid_count = 0;
    for (int i = 0; i < count; i++) {
        const REDIS_YK* yk_cmd = &commands[i];

        /* Skip zero-initialized commands (check if deviceId is non-zero) */
        if (yk_cmd->deviceId == 0) {
            continue;
        }

        valid_count++;
        /* Publish command with REDIS_YK structure pointer */
        data_collection_publish_yk(client, yk_cmd, callback, user_data);
    }

    if (valid_count > 0) {
        printf("[PUB] YK batch: %d commands published\n", valid_count);
    }
}

/* Publish scheduled YT commands */
void data_collection_publish_scheduled_yt(struct RedisClient* client, const REDIS_YT* commands, int count, PublishCommandCallback callback, void* user_data) {
	if (!client || !client->redis_ctx || !commands) {
		printf("[ERR] YT scheduled: invalid params\n");
		return;
	}

    int valid_count = 0;
    for (int i = 0; i < count; i++) {
        const REDIS_YT* yt_cmd = &commands[i];

        /* Skip zero-initialized commands (check if deviceId is non-zero) */
        if (yt_cmd->deviceId == 0) {
            continue;
        }

        valid_count++;
        /* Publish command with REDIS_YT structure pointer */
        data_collection_publish_yt(client, yt_cmd, callback, user_data);
    }

    if (valid_count > 0) {
        printf("[PUB] YT batch: %d commands published\n", valid_count);
    }
}

