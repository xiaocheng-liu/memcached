/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/resource.h>
#include <signal.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t maintenance_lock = PTHREAD_MUTEX_INITIALIZER;

/* 哈希表的桶数量 = 2^hashpower（动态扩容用） */
unsigned int hashpower = HASHPOWER_DEFAULT;

#define hashsize(n) ((uint64_t)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/* 主哈希表：除扩容期间外，所有查找都在这里进行 */
static item** primary_hashtable = 0;

/*
 * 旧的哈希表：扩容期间尚未迁移到新表的 key 仍保留在此处。
 */
static item** old_hashtable = 0;

/* 是否正处于扩容（rehash）过程中 */
static bool expanding = false;

/*
 * 扩容时按“桶”粒度逐步迁移：expand_bucket 记录已经迁移到的桶下标，
 * 范围 0 .. hashsize(hashpower - 1) - 1。
 */
static uint64_t expand_bucket = 0;

/* 初始化哈希表：分配 primary_hashtable 并登记统计信息 */
void assoc_init(const int hashtable_init) {
    if (hashtable_init) {
        hashpower = hashtable_init;
    }
    primary_hashtable = calloc(hashsize(hashpower), sizeof(void *));
    if (! primary_hashtable) {
        fprintf(stderr, "Failed to init hashtable.\n");
        exit(EXIT_FAILURE);
    }
    STATS_LOCK();
    stats_state.hash_power_level = hashpower;
    stats_state.hash_bytes = hashsize(hashpower) * sizeof(void *);
    STATS_UNLOCK();
}

/* 按 key（及预计算的哈希值 hv）在哈希表中查找 item，沿冲突链比较 key。
 * 扩容进行中且旧桶尚未迁移时，会去 old_hashtable 查找。 */
item *assoc_find(const char *key, const size_t nkey, const uint32_t hv) {
    item *it;
    uint64_t oldbucket;

    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it = old_hashtable[oldbucket];
    } else {
        it = primary_hashtable[hv & hashmask(hashpower)];
    }

    item *ret = NULL;
#ifdef ENABLE_DTRACE
    int depth = 0;
#endif
    while (it) {
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
#ifdef ENABLE_DTRACE
        ++depth;
#endif
    }
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

static item** _hashitem_before (const char *key, const size_t nkey, const uint32_t hv) {
    item **pos;
    uint64_t oldbucket;

    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        pos = &old_hashtable[oldbucket];
    } else {
        pos = &primary_hashtable[hv & hashmask(hashpower)];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* 把哈希表容量翻倍（下一个 2 的幂）：把旧表挂为 old_hashtable，分配新表，
 * 随后由维护线程逐步迁移，扩容期间读写仍可正常工作。 */
static void assoc_expand(void) {
    old_hashtable = primary_hashtable;

    primary_hashtable = calloc(hashsize(hashpower + 1), sizeof(void *));
    if (primary_hashtable) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashpower++;
        expanding = true;
        expand_bucket = 0;
        STATS_LOCK();
        stats_state.hash_power_level = hashpower;
        stats_state.hash_bytes += hashsize(hashpower) * sizeof(void *);
        stats_state.hash_is_expanding = true;
        STATS_UNLOCK();
    } else {
        primary_hashtable = old_hashtable;
        /* Bad news, but we can keep running. */
    }
}

void assoc_start_expand(uint64_t curr_items) {
    if (pthread_mutex_trylock(&maintenance_lock) == 0) {
        if (curr_items > (hashsize(hashpower) * 3) / 2 && hashpower < HASHPOWER_MAX) {
            pthread_cond_signal(&maintenance_cond);
        }
        pthread_mutex_unlock(&maintenance_lock);
    }
}

/* 在哈希表中插入 item（要求 key 此前不存在）。扩容中且旧桶尚未迁移时，
 * 先插入 old_hashtable，等待迁移线程稍后搬入新表。 */
int assoc_insert(item *it, const uint32_t hv) {
    uint64_t oldbucket;

//    assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* 不应插入已存在的 key */

    if (expanding &&
        (oldbucket = (hv & hashmask(hashpower - 1))) >= expand_bucket)
    {
        it->h_next = old_hashtable[oldbucket];
        old_hashtable[oldbucket] = it;
    } else {
        it->h_next = primary_hashtable[hv & hashmask(hashpower)];
        primary_hashtable[hv & hashmask(hashpower)] = it;
    }

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey);
    return 1;
}

/* 从哈希表中删除指定 key 的 item：找到其前驱指针并跳过它 */
void assoc_delete(const char *key, const size_t nkey, const uint32_t hv) {
    item **before = _hashitem_before(key, nkey, hv);

    if (*before) {
        item *nxt;
        /* DTrace 探针不能作为最后一条指令，否则可能被编译器尾优化掉 */
        MEMCACHED_ASSOC_DELETE(key, nkey);
        nxt = (*before)->h_next;
        (*before)->h_next = 0;   /* 多半无意义，但图个安心 */
        *before = nxt;
        return;
    }
    /* 注意：实际不会走到这里，调用方只会删除找得到的东西 */
    assert(*before != 0);
}


static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

/* 哈希表维护线程：在扩容进行时，按桶粒度把 old_hashtable 中的 item
 * 重新哈希并搬入新表，搬完即释放旧表。也响应 assoc_start_expand 的扩容信号。 */
static void *assoc_maintenance_thread(void *arg) {

    mutex_lock(&maintenance_lock);
    while (do_run_maintenance_thread) {
        int ii = 0;

        /* 只有一个扩容线程，无需全局锁 */
        for (ii = 0; ii < hash_bulk_move && expanding; ++ii) {
            item *it, *next;
            uint64_t bucket;
            void *item_lock = NULL;

            /* 桶号 = hv 的低 N 位，而 item 锁的编号 = hv 的低 M 位（N > M）。
             * 因此迁移一个桶只需取一把 item_lock 即可，无需更多锁，巧妙！ */
            if ((item_lock = item_trylock(expand_bucket))) {
                    for (it = old_hashtable[expand_bucket]; NULL != it; it = next) {
                        next = it->h_next;
                        bucket = hash(ITEM_key(it), it->nkey) & hashmask(hashpower);
                        it->h_next = primary_hashtable[bucket];
                        primary_hashtable[bucket] = it;
                    }

                    old_hashtable[expand_bucket] = NULL;

                    expand_bucket++;
                    if (expand_bucket == hashsize(hashpower - 1)) {
                        expanding = false;
                        free(old_hashtable);
                        STATS_LOCK();
                        stats_state.hash_bytes -= hashsize(hashpower - 1) * sizeof(void *);
                        stats_state.hash_is_expanding = false;
                        STATS_UNLOCK();
                        if (settings.verbose > 1)
                            fprintf(stderr, "Hash table expansion done\n");
                    }

            } else {
                usleep(10*1000 - 1);
            }

            if (item_lock) {
                item_trylock_unlock(item_lock);
                item_lock = NULL;
            }
        }

        if (!expanding) {
            /* We are done expanding.. just wait for next invocation */
            pthread_cond_wait(&maintenance_cond, &maintenance_lock);
            /* assoc_expand() swaps out the hash table entirely, so we need
             * all threads to not hold any references related to the hash
             * table while this happens.
             * This is instead of a more complex, possibly slower algorithm to
             * allow dynamic hash table expansion without causing significant
             * wait times.
             */
            if (do_run_maintenance_thread) {
                pause_threads(PAUSE_ALL_THREADS);
                assoc_expand();
                pause_threads(RESUME_ALL_THREADS);
            }
        }
    }
    mutex_unlock(&maintenance_lock);
    return NULL;
}

static pthread_t maintenance_tid;

int start_assoc_maintenance_thread(void) {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }

    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    thread_setname(maintenance_tid, "mc-assocmaint");
    return 0;
}

void stop_assoc_maintenance_thread(void) {
    mutex_lock(&maintenance_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    mutex_unlock(&maintenance_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}

struct assoc_iterator {
    uint64_t bucket;
    item *it;
    item *next;
    bool bucket_locked;
};

void *assoc_get_iterator(void) {
    struct assoc_iterator *iter = calloc(1, sizeof(struct assoc_iterator));
    if (iter == NULL) {
        return NULL;
    }
    // this will hang the caller while a hash table expansion is running.
    if (mutex_trylock(&maintenance_lock) == 0) {
        return iter;
    } else {
        free(iter);
        return NULL;
    }
}

bool assoc_iterate(void *iterp, item **it) {
    struct assoc_iterator *iter = (struct assoc_iterator *) iterp;
    *it = NULL;
    // - if locked bucket and next, update next and return
    if (iter->bucket_locked) {
        if (iter->next != NULL) {
            iter->it = iter->next;
            iter->next = iter->it->h_next;
            *it = iter->it;
        } else {
            // unlock previous bucket, if any
            item_unlock(iter->bucket);
            // iterate the bucket post since it starts at 0.
            iter->bucket++;
            iter->bucket_locked = false;
            *it = NULL;
        }
        return true;
    }

    // - loop until we hit the end or find something.
    if (iter->bucket != hashsize(hashpower)) {
        // - lock next bucket
        item_lock(iter->bucket);
        iter->bucket_locked = true;
        // - only check the primary hash table since expand is blocked.
        iter->it = primary_hashtable[iter->bucket];
        if (iter->it != NULL) {
            // - set it, next and return
            iter->next = iter->it->h_next;
            *it = iter->it;
        } else {
            // - nothing found in this bucket, try next.
            item_unlock(iter->bucket);
            iter->bucket_locked = false;
            iter->bucket++;
        }
    } else {
        return false;
    }

    return true;
}

void assoc_iterate_final(void *iterp) {
    struct assoc_iterator *iter = (struct assoc_iterator *) iterp;
    if (iter->bucket_locked) {
        item_unlock(iter->bucket);
    }
    mutex_unlock(&maintenance_lock);
    free(iter);
}
