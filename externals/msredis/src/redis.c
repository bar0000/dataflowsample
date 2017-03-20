/*
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "redis.h"
#include "slowlog.h"
#include "bio.h"

#ifdef HAVE_BACKTRACE
#include <execinfo.h>
#include <ucontext.h>
#endif /* HAVE_BACKTRACE */

#include <time.h>
#include <signal.h>
#ifdef _WIN32
#include <locale.h>
#define LOG_LOCAL0 0
#else
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#ifndef _WIN32
#include <pthread.h>
#endif

/* Our shared "common" objects */

struct sharedObjectsStruct shared;

/* Global vars that are actually used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

/*================================= Globals ================================= */

/* Global vars */
struct redisServer server; /* server global state */
struct redisCommand *commandTable;
struct redisCommand readonlyCommandTable[] = {
    {"get",getCommand,2,0,NULL,1,1,1},
    {"set",setCommand,3,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"setnx",setnxCommand,3,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"setex",setexCommand,4,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"append",appendCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"strlen",strlenCommand,2,0,NULL,1,1,1},
    {"del",delCommand,-2,0,NULL,0,0,0},
    {"exists",existsCommand,2,0,NULL,1,1,1},
    {"setbit",setbitCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"getbit",getbitCommand,3,0,NULL,1,1,1},
    {"setrange",setrangeCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"getrange",getrangeCommand,4,0,NULL,1,1,1},
    {"substr",getrangeCommand,4,0,NULL,1,1,1},
    {"incr",incrCommand,2,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"decr",decrCommand,2,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"mget",mgetCommand,-2,0,NULL,1,-1,1},
    {"rpush",rpushCommand,-3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lpush",lpushCommand,-3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"rpushx",rpushxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lpushx",lpushxCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"linsert",linsertCommand,5,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"rpop",rpopCommand,2,0,NULL,1,1,1},
    {"lpop",lpopCommand,2,0,NULL,1,1,1},
    {"brpop",brpopCommand,-3,0,NULL,1,1,1},
    {"brpoplpush",brpoplpushCommand,4,REDIS_CMD_DENYOOM,NULL,1,2,1},
    {"blpop",blpopCommand,-3,0,NULL,1,1,1},
    {"llen",llenCommand,2,0,NULL,1,1,1},
    {"lindex",lindexCommand,3,0,NULL,1,1,1},
    {"lset",lsetCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"lrange",lrangeCommand,4,0,NULL,1,1,1},
    {"ltrim",ltrimCommand,4,0,NULL,1,1,1},
    {"lrem",lremCommand,4,0,NULL,1,1,1},
    {"rpoplpush",rpoplpushCommand,3,REDIS_CMD_DENYOOM,NULL,1,2,1},
    {"sadd",saddCommand,-3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"srem",sremCommand,-3,0,NULL,1,1,1},
    {"smove",smoveCommand,4,0,NULL,1,2,1},
    {"sismember",sismemberCommand,3,0,NULL,1,1,1},
    {"scard",scardCommand,2,0,NULL,1,1,1},
    {"spop",spopCommand,2,0,NULL,1,1,1},
    {"srandmember",srandmemberCommand,2,0,NULL,1,1,1},
    {"sinter",sinterCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sinterstore",sinterstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"sunion",sunionCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sunionstore",sunionstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"sdiff",sdiffCommand,-2,REDIS_CMD_DENYOOM,NULL,1,-1,1},
    {"sdiffstore",sdiffstoreCommand,-3,REDIS_CMD_DENYOOM,NULL,2,-1,1},
    {"smembers",sinterCommand,2,0,NULL,1,1,1},
    {"zadd",zaddCommand,-4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zincrby",zincrbyCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"zrem",zremCommand,-3,0,NULL,1,1,1},
    {"zremrangebyscore",zremrangebyscoreCommand,4,0,NULL,1,1,1},
    {"zremrangebyrank",zremrangebyrankCommand,4,0,NULL,1,1,1},
    {"zunionstore",zunionstoreCommand,-4,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"zinterstore",zinterstoreCommand,-4,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"zrange",zrangeCommand,-4,0,NULL,1,1,1},
    {"zrangebyscore",zrangebyscoreCommand,-4,0,NULL,1,1,1},
    {"zrevrangebyscore",zrevrangebyscoreCommand,-4,0,NULL,1,1,1},
    {"zcount",zcountCommand,4,0,NULL,1,1,1},
    {"zrevrange",zrevrangeCommand,-4,0,NULL,1,1,1},
    {"zcard",zcardCommand,2,0,NULL,1,1,1},
    {"zscore",zscoreCommand,3,0,NULL,1,1,1},
    {"zrank",zrankCommand,3,0,NULL,1,1,1},
    {"zrevrank",zrevrankCommand,3,0,NULL,1,1,1},
    {"hset",hsetCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hsetnx",hsetnxCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hget",hgetCommand,3,0,NULL,1,1,1},
    {"hmset",hmsetCommand,-4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hmget",hmgetCommand,-3,0,NULL,1,1,1},
    {"hincrby",hincrbyCommand,4,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"hdel",hdelCommand,-3,0,NULL,1,1,1},
    {"hlen",hlenCommand,2,0,NULL,1,1,1},
    {"hkeys",hkeysCommand,2,0,NULL,1,1,1},
    {"hvals",hvalsCommand,2,0,NULL,1,1,1},
    {"hgetall",hgetallCommand,2,0,NULL,1,1,1},
    {"hexists",hexistsCommand,3,0,NULL,1,1,1},
    {"incrby",incrbyCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"decrby",decrbyCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"getset",getsetCommand,3,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"mset",msetCommand,-3,REDIS_CMD_DENYOOM,NULL,1,-1,2},
    {"msetnx",msetnxCommand,-3,REDIS_CMD_DENYOOM,NULL,1,-1,2},
    {"randomkey",randomkeyCommand,1,0,NULL,0,0,0},
    {"select",selectCommand,2,0,NULL,0,0,0},
    {"move",moveCommand,3,0,NULL,1,1,1},
    {"rename",renameCommand,3,0,NULL,1,1,1},
    {"renamenx",renamenxCommand,3,0,NULL,1,1,1},
    {"expire",expireCommand,3,0,NULL,0,0,0},
    {"expireat",expireatCommand,3,0,NULL,0,0,0},
    {"keys",keysCommand,2,0,NULL,0,0,0},
    {"dbsize",dbsizeCommand,1,0,NULL,0,0,0},
    {"auth",authCommand,2,0,NULL,0,0,0},
    {"ping",pingCommand,1,0,NULL,0,0,0},
    {"echo",echoCommand,2,0,NULL,0,0,0},
    {"save",saveCommand,1,0,NULL,0,0,0},
    {"bgsave",bgsaveCommand,1,0,NULL,0,0,0},
    {"bgrewriteaof",bgrewriteaofCommand,1,0,NULL,0,0,0},
    {"shutdown",shutdownCommand,1,0,NULL,0,0,0},
    {"lastsave",lastsaveCommand,1,0,NULL,0,0,0},
    {"type",typeCommand,2,0,NULL,1,1,1},
    {"multi",multiCommand,1,0,NULL,0,0,0},
    {"exec",execCommand,1,REDIS_CMD_DENYOOM,NULL,0,0,0},
    {"discard",discardCommand,1,0,NULL,0,0,0},
    {"sync",syncCommand,1,0,NULL,0,0,0},
    {"flushdb",flushdbCommand,1,0,NULL,0,0,0},
    {"flushall",flushallCommand,1,0,NULL,0,0,0},
    {"sort",sortCommand,-2,REDIS_CMD_DENYOOM,NULL,1,1,1},
    {"info",infoCommand,1,0,NULL,0,0,0},
    {"monitor",monitorCommand,1,0,NULL,0,0,0},
    {"ttl",ttlCommand,2,0,NULL,1,1,1},
    {"persist",persistCommand,2,0,NULL,1,1,1},
    {"slaveof",slaveofCommand,3,0,NULL,0,0,0},
    {"debug",debugCommand,-2,0,NULL,0,0,0},
    {"config",configCommand,-2,0,NULL,0,0,0},
    {"subscribe",subscribeCommand,-2,0,NULL,0,0,0},
    {"unsubscribe",unsubscribeCommand,-1,0,NULL,0,0,0},
    {"psubscribe",psubscribeCommand,-2,0,NULL,0,0,0},
    {"punsubscribe",punsubscribeCommand,-1,0,NULL,0,0,0},
    {"publish",publishCommand,3,REDIS_CMD_FORCE_REPLICATION,NULL,0,0,0},
    {"watch",watchCommand,-2,0,NULL,0,0,0},
    {"unwatch",unwatchCommand,1,0,NULL,0,0,0},
    {"object",objectCommand,-2,0,NULL,0,0,0},
    {"client",clientCommand,-2,0,NULL,0,0,0},
    {"slowlog",slowlogCommand,-2,0,NULL,0,0,0}
};

/*============================ Utility functions ============================ */

void redisLog(int level, const char *fmt, ...) {
#ifndef _WIN32
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
#endif
    const char *c = ".-*#";
    time_t now = time(NULL);
    va_list ap;
    FILE *fp;
    char buf[64];
    char msg[REDIS_MAX_LOGMSG_LEN];

    if (level < server.verbosity) return;

    fp = (server.logfile == NULL) ? stdout : fopen(server.logfile,"a");
    if (!fp) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    strftime(buf,sizeof(buf),"%d %b %H:%M:%S",localtime(&now));
    fprintf(fp,"[%d] %s %c %s\n",(int)getpid(),buf,c[level],msg);
    fflush(fp);

    if (server.logfile) fclose(fp);

#ifndef _WIN32
    if (server.syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
#endif
}

/* Redis generally does not try to recover from out of memory conditions
 * when allocating objects or strings, it is not clear if it will be possible
 * to report this condition to the client since the networking layer itself
 * is based on heap allocation for send buffers, so we simply abort.
 * At least the code will be simpler to read... */
void oom(const char *msg) {
    redisLog(REDIS_WARNING, "%s: Out of memory\n",msg);
    sleep(1);
    abort();
}

#ifdef _WIN32
/* Misc Windows house keeping */
void win32Cleanup(void) {

    zmalloc_free_used_memory_mutex();

    /* Clear winsocks */
    WSACleanup();
}
#endif /* _WIN32 */

/*====================== Hash table type implementation  ==================== */

/* This is an hash table type that uses the SDS dynamic strings libary as
 * keys and radis objects as values (objects can hold SDS strings,
 * lists, sets). */

void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = (int)sdslen((sds)key1);
    l2 = (int)sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

/* A case insensitive version used for the command lookup table. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp(key1, key2) == 0;
}

void dictRedisObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Values of swapped out keys as set to NULL */
    decrRefCount(val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree(val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = key1, *o2 = key2;
    return dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
}

unsigned int dictObjHash(const void *key) {
    const robj *o = key;
    return dictGenHashFunction(o->ptr, (int)sdslen((sds)o->ptr));
}

unsigned int dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, (int)sdslen((char*)key));
}

unsigned int dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, (int)sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == REDIS_ENCODING_INT &&
        o2->encoding == REDIS_ENCODING_INT)
            return o1->ptr == o2->ptr;

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,o1->ptr,o2->ptr);
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

unsigned int dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (o->encoding == REDIS_ENCODING_RAW) {
        return dictGenHashFunction(o->ptr, (int)sdslen((sds)o->ptr));
    } else {
        if (o->encoding == REDIS_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)o->ptr);
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            unsigned int hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(o->ptr, (int)sdslen((sds)o->ptr));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Sets type */
dictType setDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictRedisObjectDestructor, /* key destructor */
    NULL                       /* val destructor */
};

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictRedisObjectDestructor   /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,           /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCaseCompare,     /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with zimpaps) */
dictType hashDictType = {
    dictEncObjHash,             /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictEncObjKeyCompare,       /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictRedisObjectDestructor   /* val destructor */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictRedisObjectDestructor,  /* key destructor */
    dictListDestructor          /* val destructor */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size && used && size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < REDIS_HT_MINFILL));
}

/* If the percentage of used slots in the HT reaches REDIS_HT_MINFILL
 * we resize the hash table to save memory */
void tryResizeHashTables(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        if (htNeedsResize(server.db[j].dict))
            dictResize(server.db[j].dict);
        if (htNeedsResize(server.db[j].expires))
            dictResize(server.db[j].expires);
    }
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every serverCron() loop in order to rehash some key. */
void incrementallyRehash(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        if (dictIsRehashing(server.db[j].dict)) {
            dictRehashMilliseconds(server.db[j].dict,1);
            break; /* already used our millisecond for this loop... */
        }
    }
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have o not
 * running childs. */
void updateDictResizePolicy(void) {
    if (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1)
        dictEnableResize();
    else
        dictDisableResize();
}

/* ======================= Cron: called every 100 ms ======================== */

/* Try to expire a few timed out keys. The algorithm used is adaptive and
 * will use few CPU cycles if there are few expiring keys, otherwise
 * it will get more aggressive to avoid that too much memory is used by
 * keys that can be removed from the keyspace. */
void activeExpireCycle(void) {
    int j;

    for (j = 0; j < server.dbnum; j++) {
        int expired;
        redisDb *db = server.db+j;

        /* Continue to expire if at the end of the cycle more than 25%
         * of the keys were expired. */
        do {
            size_t num = dictSize(db->expires);
            time_t now = time(NULL);

            expired = 0;
            if (num > REDIS_EXPIRELOOKUPS_PER_CRON)
                num = REDIS_EXPIRELOOKUPS_PER_CRON;
            while (num--) {
                dictEntry *de;
                time_t t;

                if ((de = dictGetRandomKey(db->expires)) == NULL) break;
                t = (time_t) dictGetEntryVal(de);
                if (now > t) {
                    sds key = dictGetEntryKey(de);
                    robj *keyobj = createStringObject(key,sdslen(key));

                    propagateExpire(db,keyobj);
                    dbDelete(db,keyobj);
                    decrRefCount(keyobj);
                    expired++;
                    server.stat_expiredkeys++;
                }
            }
        } while (expired > REDIS_EXPIRELOOKUPS_PER_CRON/4);
    }
}

void updateLRUClock(void) {
    server.lruclock = ((unsigned long)time(NULL)/REDIS_LRU_CLOCK_RESOLUTION) &
                                                REDIS_LRU_CLOCK_MAX;
}

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int j, loops = server.cronloops;
    REDIS_NOTUSED(eventLoop);
    REDIS_NOTUSED(id);
    REDIS_NOTUSED(clientData);

    /* We take a cached value of the unix time in the global state because
     * with virtual memory and aging there is to store the current time
     * in objects at every object access, and accuracy is not needed.
     * To access a global var is faster than calling time(NULL) */
    server.unixtime = time(NULL);

    /* We have just 22 bits per object for LRU information.
     * So we use an (eventually wrapping) LRU clock with 10 seconds resolution.
     * 2^22 bits with 10 seconds resoluton is more or less 1.5 years.
     *
     * Note that even if this will wrap after 1.5 years it's not a problem,
     * everything will still work but just some object will appear younger
     * to Redis. But for this to happen a given object should never be touched
     * for 1.5 years.
     *
     * Note that you can change the resolution altering the
     * REDIS_LRU_CLOCK_RESOLUTION define.
     */
    updateLRUClock();

    /* Record the max memory used since the server was started. */
    if (zmalloc_used_memory() > server.stat_peak_memory)
        server.stat_peak_memory = zmalloc_used_memory();

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    if (server.shutdown_asap) {
        if (prepareForShutdown() == REDIS_OK) exit(0);
        redisLog(REDIS_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
    }

    /* Show some info about non-empty databases */
    for (j = 0; j < server.dbnum; j++) {
        long long size, used, vkeys;

        size = dictSlots(server.db[j].dict);
        used = dictSize(server.db[j].dict);
        vkeys = dictSize(server.db[j].expires);
        if (!(loops % 50) && (used || vkeys)) {
            redisLog(REDIS_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",j,used,vkeys,size);
            /* dictPrintStats(server.dict); */
        }
    }

    /* We don't want to resize the hash tables while a bacground saving
     * is in progress: the saving child is created using fork() that is
     * implemented with a copy-on-write semantic in most modern systems, so
     * if we resize the HT while there is the saving child at work actually
     * a lot of memory movements in the parent will cause a lot of pages
     * copied. */
    if (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1) {
        if (!(loops % 10)) tryResizeHashTables();
        if (server.activerehashing) incrementallyRehash();
    }

    /* Show information about connected clients */
    if (!(loops % 50)) {
#ifdef _WIN32
        redisLog(REDIS_VERBOSE,"%d clients connected (%d slaves), %llu bytes in use",
            listLength(server.clients)-listLength(server.slaves),
            listLength(server.slaves),
            (unsigned long long)zmalloc_used_memory());
#else
        redisLog(REDIS_VERBOSE,"%d clients connected (%d slaves), %zu bytes in use",
            listLength(server.clients)-listLength(server.slaves),
            listLength(server.slaves),
            zmalloc_used_memory());
#endif
    }

    /* Close connections of timedout clients */
    if ((server.maxidletime && !(loops % 100)) || server.bpop_blocked_clients)
        closeTimedoutClients();

    /* Start a scheduled AOF rewrite if this was requested by the user while
     * a BGSAVE was in progress. */
    if (server.bgsavechildpid == -1 && server.bgrewritechildpid == -1 &&
        server.aofrewrite_scheduled)
    {
        rewriteAppendOnlyFileBackground();
    }

    /* Check if a background saving or AOF rewrite in progress terminated */
    if (server.bgsavechildpid != -1 || server.bgrewritechildpid != -1) {
#ifdef _WIN32
        if (server.rdbbkgdfsave.state == BKSAVE_SUCCESS) {
            if (server.bgsavechildpid != -1) {
                backgroundSaveDoneHandler(0);
            } else {
                backgroundRewriteDoneHandler(0);
            }
        } else if (server.rdbbkgdfsave.state == BKSAVE_FAILED) {
            if (server.bgsavechildpid != -1) {
                backgroundSaveDoneHandler(0x100);
            } else {
                backgroundRewriteDoneHandler(0x100);
            }
        }
#else
        int statloc;
        pid_t pid;

        if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            if (pid == server.bgsavechildpid) {
                backgroundSaveDoneHandler(statloc);
            } else {
                backgroundRewriteDoneHandler(statloc);
            }
            updateDictResizePolicy();
        }
#endif
    } else {
         time_t now = time(NULL);

        /* If there is not a background saving in progress check if
         * we have to save now */
         for (j = 0; j < server.saveparamslen; j++) {
            struct saveparam *sp = server.saveparams+j;

            if (server.dirty >= sp->changes &&
                now-server.lastsave > sp->seconds) {
                redisLog(REDIS_NOTICE,"%d changes in %d seconds. Saving...",
                    sp->changes, sp->seconds);
                rdbSaveBackground(server.dbfilename);
                break;
            }
         }

         /* Trigger an AOF rewrite if needed */
         if (server.bgsavechildpid == -1 &&
             server.bgrewritechildpid == -1 &&
             server.auto_aofrewrite_perc &&
             server.appendonly_current_size > server.auto_aofrewrite_min_size)
         {
            long long base = server.auto_aofrewrite_base_size ?
                            server.auto_aofrewrite_base_size : 1;
            long long growth = (server.appendonly_current_size*100/base) - 100;
            if (growth >= server.auto_aofrewrite_perc) {
                redisLog(REDIS_NOTICE,"Starting automatic rewriting of AOF on %lld%% growth",growth);
                rewriteAppendOnlyFileBackground();
            }
        }
    }


    /* If we postponed an AOF buffer flush, let's try to do it every time the
     * cron function is called. */
    if (server.aof_flush_postponed_start) flushAppendOnlyFile(0);

    /* Expire a few keys per cycle, only if this is a master.
     * On slaves we wait for DEL operations synthesized by the master
     * in order to guarantee a strict consistency. */
    if (server.masterhost == NULL) activeExpireCycle();

    /* Replication cron function -- used to reconnect to master and
     * to detect transfer failures. */
    if (!(loops % 10)) replicationCron();

    server.cronloops++;
    return 100;
}

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    listNode *ln;
    redisClient *c;
    REDIS_NOTUSED(eventLoop);

    /* Try to process pending commands for clients that were just unblocked. */
    while (listLength(server.unblocked_clients)) {
        ln = listFirst(server.unblocked_clients);
        redisAssert(ln != NULL);
        c = ln->value;
        listDelNode(server.unblocked_clients,ln);
        c->flags &= ~REDIS_UNBLOCKED;

        /* Process remaining data in the input buffer. */
        if (c->querybuf && sdslen(c->querybuf) > 0)
            processInputBuffer(c);
    }

    /* Write the AOF buffer on disk */
    flushAppendOnlyFile(0);
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
    int j;

    shared.crlf = createObject(REDIS_STRING,sdsnew("\r\n"));
    shared.ok = createObject(REDIS_STRING,sdsnew("+OK\r\n"));
    shared.err = createObject(REDIS_STRING,sdsnew("-ERR\r\n"));
    shared.emptybulk = createObject(REDIS_STRING,sdsnew("$0\r\n\r\n"));
    shared.czero = createObject(REDIS_STRING,sdsnew(":0\r\n"));
    shared.cone = createObject(REDIS_STRING,sdsnew(":1\r\n"));
    shared.cnegone = createObject(REDIS_STRING,sdsnew(":-1\r\n"));
    shared.nullbulk = createObject(REDIS_STRING,sdsnew("$-1\r\n"));
    shared.nullmultibulk = createObject(REDIS_STRING,sdsnew("*-1\r\n"));
    shared.emptymultibulk = createObject(REDIS_STRING,sdsnew("*0\r\n"));
    shared.pong = createObject(REDIS_STRING,sdsnew("+PONG\r\n"));
    shared.queued = createObject(REDIS_STRING,sdsnew("+QUEUED\r\n"));
    shared.wrongtypeerr = createObject(REDIS_STRING,sdsnew(
        "-ERR Operation against a key holding the wrong kind of value\r\n"));
    shared.nokeyerr = createObject(REDIS_STRING,sdsnew(
        "-ERR no such key\r\n"));
    shared.syntaxerr = createObject(REDIS_STRING,sdsnew(
        "-ERR syntax error\r\n"));
    shared.sameobjecterr = createObject(REDIS_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n"));
    shared.outofrangeerr = createObject(REDIS_STRING,sdsnew(
        "-ERR index out of range\r\n"));
    shared.loadingerr = createObject(REDIS_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory\r\n"));
    shared.space = createObject(REDIS_STRING,sdsnew(" "));
    shared.colon = createObject(REDIS_STRING,sdsnew(":"));
    shared.plus = createObject(REDIS_STRING,sdsnew("+"));

    for (j = 0; j < REDIS_SHARED_SELECT_CMDS; j++) {
        shared.select[j] = createObject(REDIS_STRING,
            sdscatprintf(sdsempty(),"select %d\r\n", j));
    }
    shared.messagebulk = createStringObject("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = createStringObject("$8\r\npmessage\r\n",14);
    shared.subscribebulk = createStringObject("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = createStringObject("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = createStringObject("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = createStringObject("$12\r\npunsubscribe\r\n",19);
    shared.mbulk3 = createStringObject("*3\r\n",4);
    shared.mbulk4 = createStringObject("*4\r\n",4);
    for (j = 0; j < REDIS_SHARED_INTEGERS; j++) {
        shared.integers[j] = createObject(REDIS_STRING,(void*)(long)j);
        shared.integers[j]->encoding = REDIS_ENCODING_INT;
    }
}

#ifdef _WIN32
#pragma warning(disable: 4723)
#endif
void initServerConfig() {
    server.arch_bits = (sizeof(void*) == 8) ? 64 : 32;
    server.port = REDIS_SERVERPORT;
    server.bindaddr = NULL;
    server.unixsocket = NULL;
    server.unixsocketperm = 0;
    server.ipfd = -1;
    server.sofd = -1;
    server.dbnum = REDIS_DEFAULT_DBNUM;
    server.verbosity = REDIS_VERBOSE;
    server.maxidletime = REDIS_MAXIDLETIME;
    server.client_max_querybuf_len = REDIS_MAX_QUERYBUF_LEN;
    server.saveparams = NULL;
    server.loading = 0;
    server.logfile = NULL; /* NULL = log on standard output */
    server.syslog_enabled = 0;
    server.syslog_ident = zstrdup("redis");
    server.syslog_facility = LOG_LOCAL0;
    server.daemonize = 0;
    server.appendonly = 0;
    server.appendfsync = APPENDFSYNC_EVERYSEC;
    server.no_appendfsync_on_rewrite = 0;
    server.auto_aofrewrite_perc = REDIS_AUTO_AOFREWRITE_PERC;
    server.auto_aofrewrite_min_size = REDIS_AUTO_AOFREWRITE_MIN_SIZE;
    server.auto_aofrewrite_base_size = 0;
    server.aofrewrite_scheduled = 0;
    server.lastfsync = time(NULL);
    server.appendfd = -1;
    server.appendseldb = -1; /* Make sure the first time will not match */
    server.aof_flush_postponed_start = 0;
    server.pidfile = zstrdup("/var/run/redis.pid");
    server.dbfilename = zstrdup("dump.rdb");
    server.appendfilename = zstrdup("appendonly.aof");
    server.requirepass = NULL;
    server.rdbcompression = 1;
    server.activerehashing = 1;
    server.maxclients = 0;
    server.bpop_blocked_clients = 0;
    server.maxmemory = 0;
    server.maxmemory_policy = REDIS_MAXMEMORY_VOLATILE_LRU;
    server.maxmemory_samples = 3;
    server.hash_max_zipmap_entries = REDIS_HASH_MAX_ZIPMAP_ENTRIES;
    server.hash_max_zipmap_value = REDIS_HASH_MAX_ZIPMAP_VALUE;
    server.list_max_ziplist_entries = REDIS_LIST_MAX_ZIPLIST_ENTRIES;
    server.list_max_ziplist_value = REDIS_LIST_MAX_ZIPLIST_VALUE;
    server.set_max_intset_entries = REDIS_SET_MAX_INTSET_ENTRIES;
    server.zset_max_ziplist_entries = REDIS_ZSET_MAX_ZIPLIST_ENTRIES;
    server.zset_max_ziplist_value = REDIS_ZSET_MAX_ZIPLIST_VALUE;
    server.shutdown_asap = 0;
    server.repl_ping_slave_period = REDIS_REPL_PING_SLAVE_PERIOD;
    server.repl_timeout = REDIS_REPL_TIMEOUT;

    updateLRUClock();
    resetServerSaveParams();

    appendServerSaveParams(60*60,1);  /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100);  /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */
    /* Replication related */
    server.isslave = 0;
    server.masterauth = NULL;
    server.masterhost = NULL;
    server.masterport = 6379;
    server.master = NULL;
    server.replstate = REDIS_REPL_NONE;
    server.repl_syncio_timeout = REDIS_REPL_SYNCIO_TIMEOUT;
    server.repl_serve_stale_data = 1;
    server.repl_down_since = time(NULL);

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    /* Command table -- we intiialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * redis.conf using the rename-command directive. */
    server.commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    server.delCommand = lookupCommandByCString("del");
    server.multiCommand = lookupCommandByCString("multi");

    /* Slow log */
    server.slowlog_log_slower_than = REDIS_SLOWLOG_LOG_SLOWER_THAN;
    server.slowlog_max_len = REDIS_SLOWLOG_MAX_LEN;

    /* Assert */
    server.assert_failed = "<no assertion failed>";
    server.assert_file = "<no file>";
    server.assert_line = 0;
    server.bug_report_start = 0;
}

void initServer() {
    int j;
#ifdef _WIN32
    HMODULE lib;
#endif

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSignalHandlers();

#ifndef _WIN32
    if (server.syslog_enabled) {
        openlog(server.syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            server.syslog_facility);
    }
#endif

#ifdef _WIN32
     /* Force binary mode on all files */
    _fmode = _O_BINARY;
    _setmode(_fileno(stdin),  _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
    _setmode(_fileno(stderr), _O_BINARY);

    /* Set C locale, forcing strtod() to work with dots */
    setlocale(LC_ALL, "C");

    /* MingGW 32 lacks declaration of RtlGenRandom, MinGw64 don't */
    lib = LoadLibraryA("advapi32.dll");
    RtlGenRandom = (RtlGenRandomFunc)GetProcAddress(lib, "SystemFunction036");

    /* Winsocks must be initialized */
    if (!w32initWinSock()) {
        redisLog(REDIS_WARNING, "Can't init WinSock2; Error code: %d", WSAGetLastError());
        exit(1);
    };
    /* ... and cleaned at application exit */
    atexit((void(*)(void)) win32Cleanup);
#endif
    server.mainthread = pthread_self();
    server.current_client = NULL;
    server.clients = listCreate();
    server.slaves = listCreate();
    server.monitors = listCreate();
    server.unblocked_clients = listCreate();
    createSharedObjects();
    server.el = aeCreateEventLoop();
    server.db = zmalloc(sizeof(redisDb)*server.dbnum);

    if (server.port != 0) {
        server.ipfd = anetTcpServer(server.neterr,server.port,server.bindaddr);
        if (server.ipfd == ANET_ERR) {
            redisLog(REDIS_WARNING, "Opening port %d: %s",
                server.port, server.neterr);
            exit(1);
        }
    }
    if (server.unixsocket != NULL) {
        unlink(server.unixsocket); /* don't care if this fails */
        server.sofd = anetUnixServer(server.neterr,server.unixsocket,server.unixsocketperm);
        if (server.sofd == ANET_ERR) {
            redisLog(REDIS_WARNING, "Opening socket: %s", server.neterr);
            exit(1);
        }
    }
    if (server.ipfd < 0 && server.sofd < 0) {
        redisLog(REDIS_WARNING, "Configured to not listen anywhere, exiting.");
        exit(1);
    }
    for (j = 0; j < server.dbnum; j++) {
        server.db[j].dict = dictCreate(&dbDictType,NULL);
        server.db[j].expires = dictCreate(&keyptrDictType,NULL);
        server.db[j].blocking_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].watched_keys = dictCreate(&keylistDictType,NULL);
        server.db[j].id = j;
    }
    server.pubsub_channels = dictCreate(&keylistDictType,NULL);
    server.pubsub_patterns = listCreate();
    listSetFreeMethod(server.pubsub_patterns,freePubsubPattern);
    listSetMatchMethod(server.pubsub_patterns,listMatchPubsubPattern);
    server.cronloops = 0;
    server.bgsavechildpid = -1;
    server.bgrewritechildpid = -1;
    server.bgrewritebuf = sdsempty();
    server.aofbuf = sdsempty();
    server.lastsave = time(NULL);
    server.dirty = 0;
    server.stat_numcommands = 0;
    server.stat_numconnections = 0;
    server.stat_expiredkeys = 0;
    server.stat_evictedkeys = 0;
    server.stat_starttime = time(NULL);
    server.stat_keyspace_misses = 0;
    server.stat_keyspace_hits = 0;
    server.stat_peak_memory = 0;
    server.stat_fork_time = 0;
    server.unixtime = time(NULL);
    aeCreateTimeEvent(server.el, 1, serverCron, NULL, NULL);
    if (server.ipfd > 0 && aeCreateFileEvent(server.el,server.ipfd,AE_READABLE,
        acceptTcpHandler,NULL) == AE_ERR) oom("creating file event");
    if (server.sofd > 0 && aeCreateFileEvent(server.el,server.sofd,AE_READABLE,
        acceptUnixHandler,NULL) == AE_ERR) oom("creating file event");

    if (server.appendonly) {
#ifdef _WIN32
        server.appendfd = open(server.appendfilename,O_WRONLY|O_APPEND|O_CREAT|_O_BINARY,_S_IREAD|_S_IWRITE);
#else
        server.appendfd = open(server.appendfilename,O_WRONLY|O_APPEND|O_CREAT,0644);
#endif
        if (server.appendfd == -1) {
            redisLog(REDIS_WARNING, "Can't open the append-only file: %s",
                strerror(errno));
            exit(1);
        }
    }

    /* 32 bit instances are limited to 4GB of address space, so if there is
     * no explicit limit in the user provided configuration we set a limit
     * at 3.5GB using maxmemory with 'noeviction' policy'. This saves
     * useless crashes of the Redis instance. */
    if (server.arch_bits == 32 && server.maxmemory == 0) {
        redisLog(REDIS_WARNING,"Warning: 32 bit instance detected but no memory limit set. Setting 3.5 GB maxmemory limit with 'noeviction' policy now.");
        server.maxmemory = 3584LL*(1024*1024); /* 3584 MB = 3.5 GB */
        server.maxmemory_policy = REDIS_MAXMEMORY_NO_EVICTION;
    }

    slowlogInit();
    bioInit();
    srand((unsigned int)(time(NULL)^getpid()));
}

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of redis.c file. */
void populateCommandTable(void) {
    int j;
    int numcommands = sizeof(readonlyCommandTable)/sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = readonlyCommandTable+j;
        int retval;

        retval = dictAdd(server.commands, sdsnew(c->name), c);
        assert(retval == DICT_OK);
    }
}

/* ====================== Commands lookup and execution ===================== */

struct redisCommand *lookupCommand(sds name) {
    return dictFetchValue(server.commands, name);
}

struct redisCommand *lookupCommandByCString(char *s) {
    struct redisCommand *cmd;
    sds name = sdsnew(s);

    cmd = dictFetchValue(server.commands, name);
    sdsfree(name);
    return cmd;
}

/* Call() is the core of Redis execution of a command */
void call(redisClient *c) {
    long long dirty, start = ustime(), duration;

    dirty = server.dirty;
    c->cmd->proc(c);
    dirty = server.dirty-dirty;
    duration = ustime()-start;
    slowlogPushEntryIfNeeded(c->argv,c->argc,duration);

    if (server.appendonly && dirty > 0)
        feedAppendOnlyFile(c->cmd,c->db->id,c->argv,c->argc);
    if ((dirty > 0 || c->cmd->flags & REDIS_CMD_FORCE_REPLICATION) &&
        listLength(server.slaves))
        replicationFeedSlaves(server.slaves,c->db->id,c->argv,c->argc);
    if (listLength(server.monitors))
        replicationFeedMonitors(server.monitors,c->db->id,c->argv,c->argc);
    server.stat_numcommands++;
}

/* If this function gets called we already read a whole
 * command, argments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * and other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroied (i.e. after QUIT). */
int processCommand(redisClient *c) {
    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    if (!strcasecmp(c->argv[0]->ptr,"quit")) {
        addReply(c,shared.ok);
        c->flags |= REDIS_CLOSE_AFTER_REPLY;
        return REDIS_ERR;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand(c->argv[0]->ptr);
    if (!c->cmd) {
        addReplyErrorFormat(c,"unknown command '%s'",
            (char*)c->argv[0]->ptr);
        return REDIS_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return REDIS_OK;
    }

    /* Check if the user is authenticated */
    if (server.requirepass && !c->authenticated && c->cmd->proc != authCommand)
    {
        addReplyError(c,"operation not permitted");
        return REDIS_OK;
    }

    /* Handle the maxmemory directive.
     *
     * First we try to free some memory if possible (if there are volatile
     * keys in the dataset). If there are not the only thing we can do
     * is returning an error. */
    if (server.maxmemory) {
        int retval = freeMemoryIfNeeded();
        if ((c->cmd->flags & REDIS_CMD_DENYOOM) && retval == REDIS_ERR) {
            addReplyError(c,
                "command not allowed when used memory > 'maxmemory'");
            return REDIS_OK;
        }
    }

    /* Only allow SUBSCRIBE and UNSUBSCRIBE in the context of Pub/Sub */
    if ((dictSize(c->pubsub_channels) > 0 || listLength(c->pubsub_patterns) > 0)
        &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand) {
        addReplyError(c,"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
        return REDIS_OK;
    }

    /* Only allow INFO and SLAVEOF when slave-serve-stale-data is no and
     * we are a slave with a broken link with master. */
    if (server.masterhost && server.replstate != REDIS_REPL_CONNECTED &&
        server.repl_serve_stale_data == 0 &&
        c->cmd->proc != infoCommand && c->cmd->proc != slaveofCommand)
    {
        addReplyError(c,
            "link with MASTER is down and slave-serve-stale-data is set to no");
        return REDIS_OK;
    }

    /* Loading DB? Return an error if the command is not INFO */
    if (server.loading && c->cmd->proc != infoCommand) {
        addReply(c, shared.loadingerr);
        return REDIS_OK;
    }

    /* Exec the command */
    if (c->flags & REDIS_MULTI &&
        c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
        c->cmd->proc != multiCommand && c->cmd->proc != watchCommand)
    {
        queueMultiCommand(c);
        addReply(c,shared.queued);
    } else {
        call(c);
    }
    return REDIS_OK;
}

/*================================== Shutdown =============================== */

int prepareForShutdown() {
    redisLog(REDIS_WARNING,"User requested shutdown...");
    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (server.bgsavechildpid != -1) {
        redisLog(REDIS_WARNING,"There is a child saving an .rdb. Killing it!");
#ifdef _WIN32
        bkgdsave_termthread();
#else
        kill(server.bgsavechildpid,SIGKILL);
#endif
        rdbRemoveTempFile(server.bgsavechildpid);
    }
    if (server.appendonly) {
        /* Kill the AOF saving child as the AOF we already have may be longer
         * but contains the full dataset anyway. */
        if (server.bgrewritechildpid != -1) {
            redisLog(REDIS_WARNING,
                "There is a child rewriting the AOF. Killing it!");
#ifdef _WIN32
            bkgdsave_termthread();
#else
            kill(server.bgrewritechildpid,SIGKILL);
#endif
        }
        /* Append only file: fsync() the AOF and exit */
        redisLog(REDIS_NOTICE,"Calling fsync() on the AOF file.");
        aof_fsync(server.appendfd);
    }
    if (server.saveparamslen > 0) {
        redisLog(REDIS_NOTICE,"Saving the final RDB snapshot before exiting.");
        /* Snapshotting. Perform a SYNC SAVE and exit */
        if (rdbSave(server.dbfilename) != REDIS_OK) {
            /* Ooops.. error saving! The best we can do is to continue
             * operating. Note that if there was a background saving process,
             * in the next cron() Redis will be notified that the background
             * saving aborted, handling special stuff like slaves pending for
             * synchronization... */
            redisLog(REDIS_WARNING,"Error trying to save the DB, can't exit.");
            return REDIS_ERR;
        }
    }
    if (server.daemonize) {
        redisLog(REDIS_NOTICE,"Removing the pid file.");
        unlink(server.pidfile);
    }
    /* Close the listening sockets. Apparently this allows faster restarts. */
#ifdef _WIN32
    if (server.ipfd != -1) closesocket(server.ipfd);
    if (server.sofd != -1) closesocket(server.sofd);
#else
    if (server.ipfd != -1) close(server.ipfd);
    if (server.sofd != -1) close(server.sofd);
#endif
    if (server.unixsocket) {
        redisLog(REDIS_NOTICE,"Removing the unix socket file.");
        unlink(server.unixsocket); /* don't care if this fails */
    }

    redisLog(REDIS_WARNING,"Redis is now ready to exit, bye bye...");
    return REDIS_OK;
}

/*================================== Commands =============================== */

void authCommand(redisClient *c) {
    if (!server.requirepass) {
        addReplyError(c,"Client sent AUTH, but no password is set");
    } else if (!strcmp(c->argv[1]->ptr, server.requirepass)) {
      c->authenticated = 1;
      addReply(c,shared.ok);
    } else {
      c->authenticated = 0;
      addReplyError(c,"invalid password");
    }
}

void pingCommand(redisClient *c) {
    addReply(c,shared.pong);
}

void echoCommand(redisClient *c) {
    addReplyBulk(c,c->argv[1]);
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
        return;
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    }
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genRedisInfoString(void) {
    sds info;
    time_t uptime = time(NULL)-server.stat_starttime;
    int j;
    char hmem[64], peak_hmem[64];
    struct rusage self_ru, c_ru;
    unsigned long lol, bib;

    getrusage(RUSAGE_SELF, &self_ru);
    getrusage(RUSAGE_CHILDREN, &c_ru);
    getClientsMaxBuffers(&lol,&bib);

    bytesToHuman(hmem,zmalloc_used_memory());
    bytesToHuman(peak_hmem,server.stat_peak_memory);
    info = sdscatprintf(sdsempty(),
        "redis_version:%s\r\n"
        "redis_git_sha1:%s\r\n"
        "redis_git_dirty:%d\r\n"
        "arch_bits:%d\r\n"
        "multiplexing_api:%s\r\n"
        "gcc_version:%d.%d.%d\r\n"
        "process_id:%ld\r\n"
        "uptime_in_seconds:%ld\r\n"
        "uptime_in_days:%ld\r\n"
        "lru_clock:%ld\r\n"
        "used_cpu_sys:%.2f\r\n"
        "used_cpu_user:%.2f\r\n"
        "used_cpu_sys_children:%.2f\r\n"
        "used_cpu_user_children:%.2f\r\n"
        "connected_clients:%d\r\n"
        "connected_slaves:%d\r\n"
#ifdef _WIN32
        "client_longest_output_list:%llu\r\n"
        "client_biggest_input_buf:%llu\r\n"
        "blocked_clients:%d\r\n"
        "used_memory:%llu\r\n"
        "used_memory_human:%s\r\n"
        "used_memory_rss:%llu\r\n"
        "used_memory_peak:%llu\r\n"        
#else
        "client_longest_output_list:%lu\r\n"
        "client_biggest_input_buf:%lu\r\n"
        "blocked_clients:%d\r\n"
        "used_memory:%zu\r\n"
        "used_memory_human:%s\r\n"
        "used_memory_rss:%zu\r\n"
        "used_memory_peak:%zu\r\n"
#endif
        "used_memory_peak_human:%s\r\n"
        "mem_fragmentation_ratio:%.2f\r\n"
        "mem_allocator:%s\r\n"
        "loading:%d\r\n"
        "aof_enabled:%d\r\n"
        "changes_since_last_save:%lld\r\n"
        "bgsave_in_progress:%d\r\n"
        "last_save_time:%ld\r\n"
        "bgrewriteaof_in_progress:%d\r\n"
        "total_connections_received:%lld\r\n"
        "total_commands_processed:%lld\r\n"
        "expired_keys:%lld\r\n"
        "evicted_keys:%lld\r\n"
        "keyspace_hits:%lld\r\n"
        "keyspace_misses:%lld\r\n"
        "pubsub_channels:%ld\r\n"
        "pubsub_patterns:%u\r\n"
        "latest_fork_usec:%lld\r\n"
        "vm_enabled:%d\r\n"
        "role:%s\r\n"
        ,REDIS_VERSION,
        redisGitSHA1(),
        strtol(redisGitDirty(),NULL,10) > 0,
        server.arch_bits,
        aeGetApiName(),
#ifdef __GNUC__
        __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
        0,0,0,
#endif
        (long) getpid(),
#ifdef _WIN32
        (long)uptime,
        (long)(uptime/(3600*24)),
        (unsigned long) server.lruclock,
        (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
        (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
        (float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000,
        (float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000,
        listLength(server.clients)-listLength(server.slaves),
        listLength(server.slaves),
        (unsigned long long)lol, 
        (unsigned long long)bib,            
        server.bpop_blocked_clients,
        (unsigned long long) zmalloc_used_memory(),
        hmem,
        (unsigned long long)zmalloc_get_rss(),
        (unsigned long long)server.stat_peak_memory,
        peak_hmem,
        zmalloc_get_fragmentation_ratio(),
        ZMALLOC_LIB,
        server.loading,
        server.appendonly,
        (long long) server.dirty,
        (int) (server.bgsavechildpid != -1),
        (long)(time_t) server.lastsave,
        (int) (server.bgrewritechildpid != -1),
        (long long) server.stat_numconnections,
        (long long) server.stat_numcommands,
        (long long) server.stat_expiredkeys,
        (long long) server.stat_evictedkeys,
        (long long) server.stat_keyspace_hits,
        (long long) server.stat_keyspace_misses,
        (long) dictSize(server.pubsub_channels),
        (unsigned int)listLength(server.pubsub_patterns),
        (long long)server.stat_fork_time,
        0,
        server.masterhost == 0 ? "master" : "slave"
#else
        uptime,
        uptime/(3600*24),
        (unsigned long) server.lruclock,
        (float)self_ru.ru_stime.tv_sec+(float)self_ru.ru_stime.tv_usec/1000000,
        (float)self_ru.ru_utime.tv_sec+(float)self_ru.ru_utime.tv_usec/1000000,
        (float)c_ru.ru_stime.tv_sec+(float)c_ru.ru_stime.tv_usec/1000000,
        (float)c_ru.ru_utime.tv_sec+(float)c_ru.ru_utime.tv_usec/1000000,
        listLength(server.clients)-listLength(server.slaves),
        listLength(server.slaves),
        lol, bib,
        server.bpop_blocked_clients,
        zmalloc_used_memory(),
        hmem,
        zmalloc_get_rss(),
        server.stat_peak_memory,
        peak_hmem,
        zmalloc_get_fragmentation_ratio(),
        ZMALLOC_LIB,
        server.loading,
        server.appendonly,
        server.dirty,
        server.bgsavechildpid != -1,
        server.lastsave,
        server.bgrewritechildpid != -1,
        server.stat_numconnections,
        server.stat_numcommands,
        server.stat_expiredkeys,
        server.stat_evictedkeys,
        server.stat_keyspace_hits,
        server.stat_keyspace_misses,
        dictSize(server.pubsub_channels),
        listLength(server.pubsub_patterns),
        server.stat_fork_time,
        0,
        server.masterhost == NULL ? "master" : "slave"
#endif
    );

    if (server.appendonly) {
        info = sdscatprintf(info,
            "aof_current_size:%lld\r\n"
            "aof_base_size:%lld\r\n"
            "aof_pending_rewrite:%d\r\n"
            "aof_buffer_length:%zu\r\n"
            "aof_pending_bio_fsync:%llu\r\n",
            (long long) server.appendonly_current_size,
            (long long) server.auto_aofrewrite_base_size,
            server.aofrewrite_scheduled,
            sdslen(server.aofbuf),
            bioPendingJobsOfType(REDIS_BIO_AOF_FSYNC));
    }

    /* List connected slaves */
    if (listLength(server.slaves)) {
        int slaveid = 0;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = listNodeValue(ln);
            char *state = NULL;
            char ip[32];
            int port;

            if (anetPeerToString(slave->fd,ip,&port) == -1) continue;
            switch(slave->replstate) {
            case REDIS_REPL_WAIT_BGSAVE_START:
            case REDIS_REPL_WAIT_BGSAVE_END:
                state = "wait_bgsave";
                break;
            case REDIS_REPL_SEND_BULK:
                state = "send_bulk";
                break;
            case REDIS_REPL_ONLINE:
                state = "online";
                break;
            }
            if (state == NULL) continue;
            info = sdscatprintf(info,"slave%d:%s,%d,%s\r\n",
                slaveid,ip,port,state);
            slaveid++;
        }
    }

    if (server.masterhost) {
        info = sdscatprintf(info,
            "master_host:%s\r\n"
            "master_port:%d\r\n"
            "master_link_status:%s\r\n"
            "master_last_io_seconds_ago:%d\r\n"
            "master_sync_in_progress:%d\r\n"
            ,server.masterhost,
            server.masterport,
            (server.replstate == REDIS_REPL_CONNECTED) ?
                "up" : "down",
            server.master ? ((int)(time(NULL)-server.master->lastinteraction)) : -1,
            server.replstate == REDIS_REPL_TRANSFER
        );

        if (server.replstate == REDIS_REPL_TRANSFER) {
            info = sdscatprintf(info,
                "master_sync_left_bytes:%ld\r\n"
                "master_sync_last_io_seconds_ago:%d\r\n"
                ,(long)server.repl_transfer_left,
                (int)(time(NULL)-server.repl_transfer_lastio)
            );
        }

        if (server.replstate != REDIS_REPL_CONNECTED) {
            info = sdscatprintf(info,
                "master_link_down_since_seconds:%ld\r\n",
                (long)time(NULL)-server.repl_down_since);
        }
    }

    if (server.loading) {
        double perc;
        time_t eta, elapsed;
        off_t remaining_bytes = server.loading_total_bytes-
                                server.loading_loaded_bytes;

        perc = ((double)server.loading_loaded_bytes /
               server.loading_total_bytes) * 100;

        elapsed = time(NULL)-server.loading_start_time;
        if (elapsed == 0) {
            eta = 1; /* A fake 1 second figure if we don't have enough info */
        } else {
            eta = (elapsed*remaining_bytes)/server.loading_loaded_bytes;
        }

        info = sdscatprintf(info,
            "loading_start_time:%ld\r\n"
            "loading_total_bytes:%llu\r\n"
            "loading_loaded_bytes:%llu\r\n"
            "loading_loaded_perc:%.2f\r\n"
            "loading_eta_seconds:%ld\r\n"
            ,(unsigned long) server.loading_start_time,
            (unsigned long long) server.loading_total_bytes,
            (unsigned long long) server.loading_loaded_bytes,
            perc,
            eta
        );
    }

    for (j = 0; j < server.dbnum; j++) {
        long long keys, vkeys;

        keys = dictSize(server.db[j].dict);
        vkeys = dictSize(server.db[j].expires);
        if (keys || vkeys) {
            info = sdscatprintf(info, "db%d:keys=%lld,expires=%lld\r\n",
                j, keys, vkeys);
        }
    }
    return info;
}

void infoCommand(redisClient *c) {
    sds info = genRedisInfoString();
    addReplySds(c,sdscatprintf(sdsempty(),"$%lu\r\n",
        (unsigned long)sdslen(info)));
    addReplySds(c,info);
    addReply(c,shared.crlf);
}

void monitorCommand(redisClient *c) {
    /* ignore MONITOR if aleady slave or in monitor mode */
    if (c->flags & REDIS_SLAVE) return;

    c->flags |= (REDIS_SLAVE|REDIS_MONITOR);
    c->slaveseldb = 0;
    listAddNodeTail(server.monitors,c);
    addReply(c,shared.ok);
}

/* ============================ Maxmemory directive  ======================== */

/* This function gets called when 'maxmemory' is set on the config file to limit
 * the max memory used by the server, before processing a command.
 *
 * The goal of the function is to free enough memory to keep Redis under the
 * configured memory limit.
 *
 * The function starts calculating how many bytes should be freed to keep
 * Redis under the limit, and enters a loop selecting the best keys to
 * evict accordingly to the configured policy.
 *
 * If all the bytes needed to return back under the limit were freed the
 * function returns REDIS_OK, otherwise REDIS_ERR is returned, and the caller
 * should block the execution of commands that will result in more memory
 * used by the server.
 */
int freeMemoryIfNeeded(void) {
    size_t mem_used, mem_tofree, mem_freed;
    int slaves = listLength(server.slaves);

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    mem_used = zmalloc_used_memory();
    if (slaves) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            redisClient *slave = listNodeValue(ln);
            unsigned long obuf_bytes = getClientOutputBufferMemoryUsage(slave);
            if (obuf_bytes > mem_used)
                mem_used = 0;
            else
                mem_used -= obuf_bytes;
        }
    }
    if (server.appendonly) {
        mem_used -= sdslen(server.aofbuf);
        mem_used -= sdslen(server.bgrewritebuf);
    }

    /* Check if we are over the memory limit. */
    if (mem_used <= server.maxmemory) return REDIS_OK;

    if (server.maxmemory_policy == REDIS_MAXMEMORY_NO_EVICTION)
        return REDIS_ERR; /* We need to free memory, but policy forbids. */

    /* Compute how much memory we need to free. */
    mem_tofree = (size_t)(mem_used - server.maxmemory);
    mem_freed = 0;
    while (mem_freed < mem_tofree) {
        int j, k, keys_freed = 0;

        for (j = 0; j < server.dbnum; j++) {
            long bestval = 0; /* just to prevent warning */
            sds bestkey = NULL;
            struct dictEntry *de;
            redisDb *db = server.db+j;
            dict *dict;

            if (server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU ||
                server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM)
            {
                dict = server.db[j].dict;
            } else {
                dict = server.db[j].expires;
            }
            if (dictSize(dict) == 0) continue;

            /* volatile-random and allkeys-random policy */
            if (server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_RANDOM ||
                server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_RANDOM)
            {
                de = dictGetRandomKey(dict);
                bestkey = dictGetEntryKey(de);
            }

            /* volatile-lru and allkeys-lru policy */
            else if (server.maxmemory_policy == REDIS_MAXMEMORY_ALLKEYS_LRU ||
                server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU)
            {
                for (k = 0; k < server.maxmemory_samples; k++) {
                    sds thiskey;
                    long thisval;
                    robj *o;

                    de = dictGetRandomKey(dict);
                    thiskey = dictGetEntryKey(de);
                    /* When policy is volatile-lru we need an additonal lookup
                     * to locate the real key, as dict is set to db->expires. */
                    if (server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_LRU)
                        de = dictFind(db->dict, thiskey);
                    o = dictGetEntryVal(de);
                    thisval = estimateObjectIdleTime(o);

                    /* Higher idle time is better candidate for deletion */
                    if (bestkey == NULL || thisval > bestval) {
                        bestkey = thiskey;
                        bestval = thisval;
                    }
                }
            }

            /* volatile-ttl */
            else if (server.maxmemory_policy == REDIS_MAXMEMORY_VOLATILE_TTL) {
                for (k = 0; k < server.maxmemory_samples; k++) {
                    sds thiskey;
                    long thisval;

                    de = dictGetRandomKey(dict);
                    thiskey = dictGetEntryKey(de);
                    thisval = (long) dictGetEntryVal(de);

                    /* Expire sooner (minor expire unix timestamp) is better
                     * candidate for deletion */
                    if (bestkey == NULL || thisval < bestval) {
                        bestkey = thiskey;
                        bestval = thisval;
                    }
                }
            }

            /* Finally remove the selected key. */
            if (bestkey) {
                long long delta;

                robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
                propagateExpire(db,keyobj);
                /* We compute the amount of memory freed by dbDelete() alone.
                 * It is possible that actually the memory needed to propagate
                 * the DEL in AOF and replication link is greater than the one
                 * we are freeing removing the key, but we can't account for
                 * that otherwise we would never exit the loop.
                 *
                 * AOF and Output buffer memory will be freed eventually so
                 * we only care about memory used by the key space. */
                delta = (long long) zmalloc_used_memory();
                dbDelete(db,keyobj);
                delta -= (long long) zmalloc_used_memory();
                mem_freed += (size_t)delta;
                server.stat_evictedkeys++;
                decrRefCount(keyobj);
                keys_freed++;

                /* When the memory to free starts to be big enough, we may
                 * start spending so much time here that is impossible to
                 * deliver data to the slaves fast enough, so we force the
                 * transmission here inside the loop. */
                if (slaves) flushSlavesOutputBuffers();
            }
        }
        if (!keys_freed) return REDIS_ERR; /* nothing to free... */
    }
    return REDIS_OK;
}

/* =================================== Main! ================================ */

#ifdef __linux__
int linuxOvercommitMemoryValue(void) {
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if (!fp) return -1;
    if (fgets(buf,64,fp) == NULL) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    return atoi(buf);
}

void linuxOvercommitMemoryWarning(void) {
    if (linuxOvercommitMemoryValue() == 0) {
        redisLog(REDIS_WARNING,"WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
    }
}
#endif /* __linux__ */

void createPidFile(void) {
    /* Try to write the pid file in a best-effort way. */
    FILE *fp = fopen(server.pidfile,"w");
    if (fp) {
        fprintf(fp,"%d\n",(int)getpid());
        fclose(fp);
    }
}

void daemonize(void) {
#ifdef _WIN32
    redisLog(REDIS_WARNING,"Windows does not support daemonize. Start Redis as service");
#else
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
#endif
}

void version() {
    printf("Redis server version %s (%s:%d)\n", REDIS_VERSION,
        redisGitSHA1(), atoi(redisGitDirty()) > 0);
    exit(0);
}

void usage() {
    fprintf(stderr,"Usage: ./redis-server [/path/to/redis.conf]\n");
    fprintf(stderr,"       ./redis-server - (read config from stdin)\n");
    fprintf(stderr,"       ./redis-server --test-memory <megabytes>\n\n");
    exit(1);
}

void memtest(size_t megabytes, int passes);

int main(int argc, char **argv) {
    time_t start;

    initServerConfig();
    if (argc >= 2 && strcmp(argv[1], "--test-memory") == 0) {
        if (argc == 3) {
            memtest(atoi(argv[2]),50);
            exit(0);
        } else {
            fprintf(stderr,"Please specify the amount of memory to test in megabytes.\n");
            fprintf(stderr,"Example: ./redis-server --test-memory 4096\n\n");
            exit(1);
        }
    }
    if (argc == 2) {
        if (strcmp(argv[1], "-v") == 0 ||
            strcmp(argv[1], "--version") == 0) version();
        if (strcmp(argv[1], "--help") == 0) usage();
        resetServerSaveParams();
        loadServerConfig(argv[1]);
    } else if ((argc > 2)) {
        usage();
    } else {
        redisLog(REDIS_WARNING,"Warning: no config file specified, using the default config. In order to specify a config file use 'redis-server /path/to/redis.conf'");
    }
    if (server.daemonize) daemonize();
    initServer();
#ifdef _WIN32
    cowInit();
#endif
    if (server.daemonize) createPidFile();
    redisLog(REDIS_NOTICE,"Server started, Redis version " REDIS_VERSION);
#ifdef __linux__
    linuxOvercommitMemoryWarning();
#endif
    start = time(NULL);
    if (server.appendonly) {
        if (loadAppendOnlyFile(server.appendfilename) == REDIS_OK)
            redisLog(REDIS_NOTICE,"DB loaded from append only file: %ld seconds",time(NULL)-start);
    } else {
        if (rdbLoad(server.dbfilename) == REDIS_OK) {
            redisLog(REDIS_NOTICE,"DB loaded from disk: %ld seconds",
                time(NULL)-start);
        } else if (errno != ENOENT) {
            redisLog(REDIS_WARNING,"Fatal error loading the DB. Exiting.");
            exit(1);
        }
    }
    if (server.ipfd > 0)
        redisLog(REDIS_NOTICE,"The server is now ready to accept connections on port %d", server.port);
    if (server.sofd > 0)
        redisLog(REDIS_NOTICE,"The server is now ready to accept connections at %s", server.unixsocket);
    aeSetBeforeSleepProc(server.el,beforeSleep);
    aeMain(server.el);
    aeDeleteEventLoop(server.el);
    return 0;
}

#ifdef HAVE_BACKTRACE
static void *getMcontextEip(ucontext_t *uc) {
#if defined(__FreeBSD__)
    return (void*) uc->uc_mcontext.mc_eip;
#elif defined(__dietlibc__)
    return (void*) uc->uc_mcontext.eip;
#elif defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
  #if __x86_64__
    return (void*) uc->uc_mcontext->__ss.__rip;
  #elif __i386__
    return (void*) uc->uc_mcontext->__ss.__eip;
  #else
    return (void*) uc->uc_mcontext->__ss.__srr0;
  #endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
  #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__rip;
  #else
    return (void*) uc->uc_mcontext->__ss.__eip;
  #endif
#elif defined(__i386__)
    return (void*) uc->uc_mcontext.gregs[14]; /* Linux 32 */
#elif defined(__X86_64__) || defined(__x86_64__)
    return (void*) uc->uc_mcontext.gregs[16]; /* Linux 64 */
#elif defined(__ia64__) /* Linux IA64 */
    return (void*) uc->uc_mcontext.sc_ip;
#else
    return NULL;
#endif
}

void bugReportStart(void) {
    if (server.bug_report_start == 0) {
        redisLog(REDIS_WARNING,
            "=== REDIS BUG REPORT START: Cut & paste starting from here ===");
        server.bug_report_start = 1;
    }
}

static void sigsegvHandler(int sig, siginfo_t *info, void *secret) {
    void *trace[100];
    char **messages = NULL;
    int i, trace_size = 0;
    ucontext_t *uc = (ucontext_t*) secret;
    sds infostring, clients;
    struct sigaction act;
    REDIS_NOTUSED(info);

    bugReportStart();
    redisLog(REDIS_WARNING,
        "    Redis %s crashed by signal: %d", REDIS_VERSION, sig);
    redisLog(REDIS_WARNING,
        "    Failed assertion: %s (%s:%d)", server.assert_failed,
                        server.assert_file, server.assert_line);

    /* Generate the stack trace */
    trace_size = backtrace(trace, 100);

    /* overwrite sigaction with caller's address */
    if (getMcontextEip(uc) != NULL) {
        trace[1] = getMcontextEip(uc);
    }
    messages = backtrace_symbols(trace, trace_size);
    redisLog(REDIS_WARNING, "--- STACK TRACE");
    for (i=1; i<trace_size; ++i)
        redisLog(REDIS_WARNING,"%s", messages[i]);

    /* Log INFO and CLIENT LIST */
    redisLog(REDIS_WARNING, "--- INFO OUTPUT");
    infostring = genRedisInfoString();
    redisLog(REDIS_WARNING, infostring);
    redisLog(REDIS_WARNING, "--- CLIENT LIST OUTPUT");
    clients = getAllClientsInfoString();
    redisLog(REDIS_WARNING, clients);
    /* Don't sdsfree() strings to avoid a crash. Memory may be corrupted. */

    /* Log CURRENT CLIENT info */
    if (server.current_client) {
        redisClient *cc = server.current_client;
        sds client;
        int j;

        redisLog(REDIS_WARNING, "--- CURRENT CLIENT INFO");
        client = getClientInfoString(cc);
        redisLog(REDIS_WARNING,"client: %s", client);
        /* Missing sdsfree(client) to avoid crash if memory is corrupted. */
        for (j = 0; j < cc->argc; j++) {
            robj *decoded;

            decoded = getDecodedObject(cc->argv[j]);
            redisLog(REDIS_WARNING,"argv[%d]: '%s'", j, (char*)decoded->ptr);
            decrRefCount(decoded);
        }
        /* Check if the first argument, usually a key, is found inside the
         * selected DB, and if so print info about the associated object. */
        if (cc->argc >= 1) {
            robj *val, *key;
            dictEntry *de;

            key = getDecodedObject(cc->argv[1]);
            de = dictFind(cc->db->dict, key->ptr);
            if (de) {
                val = dictGetEntryVal(de);
                redisLog(REDIS_WARNING,"key '%s' found in DB containing the following object:", key->ptr);
                redisLogObjectDebugInfo(val);
            }
            decrRefCount(key);
        }
    }

    redisLog(REDIS_WARNING,
"=== REDIS BUG REPORT END. Make sure to include from START to END. ===\n\n"
"       Please report the crash opening an issue on github:\n\n"
"           http://github.com/antirez/redis/issues\n\n"
"  Suspect RAM error? Use redis-server --test-memory to veryfy it.\n\n"
);
    /* free(messages); Don't call free() with possibly corrupted memory. */
    if (server.daemonize) unlink(server.pidfile);

    /* Make sure we exit with the right signal at the end. So for instance
     * the core will be dumped if enabled. */
    sigemptyset (&act.sa_mask);
    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
     * is used. Otherwise, sa_handler is used */
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction (sig, &act, NULL);
    kill(getpid(),sig);
}
#endif /* HAVE_BACKTRACE */

static void sigtermHandler(int sig) {
    REDIS_NOTUSED(sig);

    redisLog(REDIS_WARNING,"Received SIGTERM, scheduling shutdown...");
    server.shutdown_asap = 1;
}

void setupSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = sigtermHandler;
    sigaction(SIGTERM, &act, NULL);

#ifdef HAVE_BACKTRACE
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
#endif
    return;
}

/* The End */