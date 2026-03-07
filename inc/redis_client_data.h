/**
 * @file redis_client_data.h
 * @brief Data collection management for Redis
 */

#ifndef REDIS_CLIENT_DATA_H
#define REDIS_CLIENT_DATA_H

#include <stdint.h>  /* for int64_t */

#ifdef __cplusplus
extern "C" {
#endif

    /* Forward declaration */
    struct RedisClient;

    /* Data type enumeration */
    typedef enum {
        DATA_TYPE_YC = 1,  /* Telemetry (遥测) */
        DATA_TYPE_YX = 2,  /* Remote signal (遥信) */
        DATA_TYPE_YT = 3,  /* Energy (电能) */
        DATA_TYPE_YK = 4   /* Remote control (遥控) */
    } DataType;

    /* Data item structure */
    typedef struct {
        int station_id;       /* Station ID */
        int point_id;        /* Point ID */
        DataType type;        /* Data type */
        int is_valid;        /* Whether this point is valid */
        double value;        /* Value (for YC) */
        int int_value;       /* Integer value (for YX) */
        int is_loaded;       /* Whether data has been loaded from Redis */
    } DataItem;

    /* Data collection structure */
    typedef struct {
        DataItem items[2048];     /* Data items array */
        int count;                /* Number of items */
        int loaded_count;          /* Number of items loaded */

        /* Callback function when all data loading is complete */
        void (*on_all_data_loaded)(void* user_data);
        void* user_data;          /* User data passed to callback */
    } DataCollection;

    /* Redis request context for async loading */
    typedef struct {
        char key[32];           /* Redis key name */
        DataCollection* collection;  /* Data collection pointer */
        int item_index;          /* Index of data item */
    } RedisRequestContext;

    /* YK (remote control) command structure - 18 bytes total */
#pragma pack(push, 1)  /* Use 1-byte alignment */
    typedef struct {
        unsigned char deviceId;       /* Device ID */
        unsigned char dataId;         /* Data ID */
        unsigned char operaterType;    /* Operation type: 1=start, 2=stop */
        unsigned short val;           /* Command value */
        unsigned short status;        /* Status */
        int64_t timestamp;           /* Timestamp (8 bytes) */
    } REDIS_YK;

    //Remote adjustment command
    typedef struct {
        int deviceId;//Device ID  
        int dataId;//Data ID
        double val;//Command value
        unsigned char status;//Command status 
        int64_t timestamp;
    } REDIS_YT;

#pragma pack(pop)

/* Callback function type: called when YK publish completes */
typedef void (*PublishCommandCallback)(const char* channel, void* user_data);

/* Callback function type: called when YK message is received */
typedef void (*SubscribeMessageCallback)(const char* channel, const REDIS_YK* yk_data, void* user_data);

/* Callback function type: called when YT message is received */
typedef void (*SubscribeYTMessageCallback)(const char* channel, const REDIS_YT* yt_data, void* user_data);

	/* API declarations */
	DataCollection* data_collection_create(void);         /* Create data collection */
	void data_collection_free(DataCollection* collection);/* Free data collection */
	int data_collection_add(DataCollection* collection, int station_id, int point_id, DataType type, int is_valid); /* Add data item */
	void data_collection_load_from_redis(DataCollection* collection, struct RedisClient* client); /* Load all data from Redis */
	DataItem* find_item_by_key(DataCollection* collection, DataType type, int station_id, int point_id); /* Find data item by key (e.g., yc:1:3) */
	void data_collection_publish_yk(struct RedisClient* client, const REDIS_YK* yk_cmd,
		PublishCommandCallback callback, void* user_data); /* Publish YK (PUB/SUB, with callback) */
	void data_collection_subscribe_yk(struct RedisClient* client, const char* channel,
		SubscribeMessageCallback callback, void* user_data); /* Subscribe to YK channel */

	/* Publish scheduled YK commands */
	void data_collection_publish_scheduled_yk(struct RedisClient* client, const REDIS_YK* commands, int count, PublishCommandCallback callback, void* user_data);

	/* Publish scheduled YT commands */
	void data_collection_publish_scheduled_yt(struct RedisClient* client, const REDIS_YT* commands, int count, PublishCommandCallback callback, void* user_data);

	/* Publish YT (PUB/SUB, with callback) */
	void data_collection_publish_yt(struct RedisClient* client, const REDIS_YT* yt_cmd,
		PublishCommandCallback callback, void* user_data);

	/* Subscribe to YT channel */
	void data_collection_subscribe_yt(struct RedisClient* client, const char* channel,
		SubscribeYTMessageCallback callback, void* user_data);

#ifdef __cplusplus
}
#endif

#endif /* REDIS_CLIENT_DATA_H */
