/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/** \file
 * The main memcached header holding commonly used data
 * structures and function prototypes.
 */
#ifndef MEMCACHED_H
#define MEMCACHED_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <event.h>
#include <netdb.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <grp.h>
#include <signal.h>
/* need this to get IOV_MAX on some platforms. */
#ifndef __need_IOV_MAX
#define __need_IOV_MAX
#endif
#include <limits.h>
/* FreeBSD 4.x doesn't have IOV_MAX exposed. */
#ifndef IOV_MAX
#if defined(__FreeBSD__) || defined(__APPLE__) || defined(__GNU__)
# define IOV_MAX 1024
/* GNU/Hurd don't set MAXPATHLEN
 * http://www.gnu.org/software/hurd/hurd/porting/guidelines.html#PATH_MAX_tt_MAX_PATH_tt_MAXPATHL */
#ifndef MAXPATHLEN
#define MAXPATHLEN 4096
#endif
#endif
#endif

#if defined(__linux__)
# define SOCK_COOKIE_ID SO_MARK
#elif defined(__FreeBSD__)
# define SOCK_COOKIE_ID SO_USER_COOKIE
#elif defined(__OpenBSD__)
# define SOCK_COOKIE_ID SO_RTABLE
#endif

#include "itoa_ljust.h"
#include "slabs_mover.h"
#include "protocol_binary.h"
#include "cache.h"
#include "logger.h"
#include "queue.h"
#include "util.h"

#ifdef EXTSTORE
#include "crc32c.h"
#endif

#include "sasl_defs.h"

/* for NAPI pinning feature */
#ifndef SO_INCOMING_NAPI_ID
#define SO_INCOMING_NAPI_ID 56
#endif

/** Maximum length of a key. */
#define KEY_MAX_LENGTH 250

/** Maximum length of a uri encoded key. */
#define KEY_MAX_URI_ENCODED_LENGTH (KEY_MAX_LENGTH  * 3 + 1)

/** Size of an incr buf. */
#define INCR_MAX_STORAGE_LEN 24

#define WRITE_BUFFER_SIZE 1024
#define READ_BUFFER_SIZE 16384
#define READ_BUFFER_CACHED 0
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8
#define UDP_DATA_SIZE 1392 // UDP_MAX_PAYLOAD_SIZE - UDP_HEADER_SIZE
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)

/* Binary protocol stuff */
#define BIN_MAX_EXTLEN 20 // length of the _incr command is currently the longest.

/* Initial power multiplier for the hash table */
#define HASHPOWER_DEFAULT 16
#define HASHPOWER_MAX 32

/* Abstract the size of an item's client flag suffix */
#ifdef LARGE_CLIENT_FLAGS
typedef uint64_t client_flags_t;
#define safe_strtoflags safe_strtoull
#else
typedef uint32_t client_flags_t;
#define safe_strtoflags safe_strtoul
#endif

/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60

/*
 * Valid range of the maximum size of an item, in bytes.
 */
#define ITEM_SIZE_MAX_LOWER_LIMIT 1024
#define ITEM_SIZE_MAX_UPPER_LIMIT 1024 * 1024 * 1024

/* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST  256 /* actual cap is 255 */
#define SLAB_GLOBAL_PAGE_POOL 0 /* magic slab class for storing pages for reassignment */
#define CHUNK_ALIGN_BYTES 8
/* slab class max is a 6-bit number, -1. */
#define MAX_NUMBER_OF_SLAB_CLASSES (63 + 1)

/** How long an object can reasonably be assumed to be locked before
    harvesting it on a low memory condition. Default: disabled. */
#define TAIL_REPAIR_TIME_DEFAULT 0

/* warning: don't use these macros with a function, as it evals its arg twice */
/* 读取 item 的 CAS（compare-and-swap）值；若未开启 CAS 则返回 0 */
#define ITEM_get_cas(i) (((i)->it_flags & ITEM_CAS) ? \
        (i)->data->cas : (uint64_t)0)

/* 设置 item 的 CAS 值（仅当该 item 启用了 ITEM_CAS 标志时生效） */
#define ITEM_set_cas(i,v) { \
    if ((i)->it_flags & ITEM_CAS) { \
        (i)->data->cas = v; \
    } \
}

/* 取得 key 的起始地址：CAS 占用 8 字节时，key 紧跟在 CAS 之后 */
#define ITEM_key(item) (((char*)&((item)->data)) \
         + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

/* 取得 suffix（客户端 flag）的起始地址：位于 key 及结尾 '\0' 之后 */
#define ITEM_suffix(item) ((char*) &((item)->data) + (item)->nkey + 1 \
         + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

/* 取得真正数据（value）的起始地址：依次跳过 key、客户端 flag、CAS */
#define ITEM_data(item) ((char*) &((item)->data) + (item)->nkey + 1 \
         + (((item)->it_flags & ITEM_CFLAGS) ? sizeof(client_flags_t) : 0) \
         + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

/* 计算 item 在内存中的总占用字节数（结构体本身 + key + value + 各种可选字段） */
#define ITEM_ntotal(item) (sizeof(struct _stritem) + (item)->nkey + 1 \
         + (item)->nbytes \
         + (((item)->it_flags & ITEM_CFLAGS) ? sizeof(client_flags_t) : 0) \
         + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))

/* 从 slabs_clsid 中分解出真正的 slab class id（低 6 位） */
#define ITEM_clsid(item) ((item)->slabs_clsid & ~(3<<6))
/* 从 slabs_clsid 中分解出 LRU id（高 2 位，表示 HOT/WARM/COLD 等 LRU 类型） */
#define ITEM_lruid(item) ((item)->slabs_clsid & (3<<6))

#define STAT_KEY_LEN 128
#define STAT_VAL_LEN 128

/** Append a simple stat with a stat name, value format and value */
#define APPEND_STAT(name, fmt, val) \
    append_stat(name, add_stats, c, fmt, val);

/** Append an indexed stat with a stat name (with format), value format
    and value */
#define APPEND_NUM_FMT_STAT(name_fmt, num, name, fmt, val)          \
    klen = snprintf(key_str, STAT_KEY_LEN, name_fmt, num, name);    \
    vlen = snprintf(val_str, STAT_VAL_LEN, fmt, val);               \
    add_stats(key_str, klen, val_str, vlen, c);

/** Common APPEND_NUM_FMT_STAT format. */
#define APPEND_NUM_STAT(num, name, fmt, val) \
    APPEND_NUM_FMT_STAT("%d:%s", num, name, fmt, val)

/** Item client flag conversion */
#define FLAGS_CONV(it, flag) { \
    if ((it)->it_flags & ITEM_CFLAGS) { \
        flag = *((client_flags_t *)ITEM_suffix((it))); \
    } else { \
        flag = 0; \
    } \
}

#define FLAGS_SIZE(item) (((item)->it_flags & ITEM_CFLAGS) ? sizeof(client_flags_t) : 0)

/**
 * Callback for any function producing stats.
 *
 * @param key the stat's key
 * @param klen length of the key
 * @param val the stat's value in an ascii form (e.g. text form of a number)
 * @param vlen length of the value
 * @parm cookie magic callback cookie
 */
typedef void (*ADD_STAT)(const char *key, const uint16_t klen,
                         const char *val, const uint32_t vlen,
                         const void *cookie);

/*
 * NOTE: If you modify this table you _MUST_ update the function state_text
 */
/**
 * Possible states of a connection.
 */
/* 连接的状态机：memcached 采用基于 libevent 的状态机处理每个客户端连接 */
enum conn_states {
    conn_listening,  /**< 监听套接字，等待新连接接入 */
    conn_new_cmd,    /**< 准备处理下一条命令（重置读取状态） */
    conn_waiting,    /**< 等待套接字可读事件 */
    conn_read,       /**< 正在读取命令行的输入 */
    conn_parse_cmd,  /**< 尝试从输入缓冲区解析一条命令 */
    conn_write,      /**< 写出一条简单响应 */
    conn_nread,      /**< 读取固定长度的字节（如 value 体） */
    conn_swallow,    /**< 丢弃不需要的字节（不存储） */
    conn_closing,    /**< 正在关闭该连接 */
    conn_mwrite,     /**< 顺序写出多个 item 响应 */
    conn_closed,     /**< 连接已关闭 */
    conn_watch,      /**< 被日志线程持有时作为 watcher */
    conn_io_queue,   /**< 等待异步处理返回响应对象 */
    conn_io_resume,  /**< 异步工作完成后准备恢复 mwrite */
    conn_io_pending, /**< 等待异步工作时被唤醒 */
    conn_max_state   /**< 最大状态值（用于断言） */
};

enum bin_substates {
    bin_no_state,
    bin_reading_set_header,
    bin_reading_cas_header,
    bin_read_set_value,
    bin_reading_get_key,
    bin_reading_stat,
    bin_reading_del_header,
    bin_reading_incr_header,
    bin_read_flush_exptime,
    bin_reading_sasl_auth,
    bin_reading_sasl_auth_data,
    bin_reading_touch_key,
};

enum protocol {
    ascii_prot = 3, /* arbitrary value. */
    binary_prot,
    negotiating_prot, /* Discovering the protocol */
#ifdef PROXY
    proxy_prot,
#endif
};

enum network_transport {
    local_transport, /* Unix sockets*/
    tcp_transport,
    udp_transport
};

enum pause_thread_types {
    PAUSE_WORKER_THREADS = 0,
    PAUSE_ALL_THREADS,
    RESUME_ALL_THREADS,
    RESUME_WORKER_THREADS
};

enum stop_reasons {
    NOT_STOP,
    GRACE_STOP,
    EXIT_NORMALLY
};

enum close_reasons {
    ERROR_CLOSE,
    NORMAL_CLOSE,
    IDLE_TIMEOUT_CLOSE,
    SHUTDOWN_CLOSE,
};

#define IS_TCP(x) (x == tcp_transport)
#define IS_UDP(x) (x == udp_transport)

#define NREAD_ADD 1
#define NREAD_SET 2
#define NREAD_REPLACE 3
#define NREAD_APPEND 4
#define NREAD_PREPEND 5
#define NREAD_CAS 6
#define NREAD_APPENDVIV 7 // specific to meta
#define NREAD_PREPENDVIV 8 // specific to meta

#define CAS_ALLOW_STALE true
#define CAS_NO_STALE false

#define LOG_TYPE_DELETE 1
#define LOG_TYPE_META_DELETE 2

enum store_item_type {
    NOT_STORED=0, STORED, EXISTS, NOT_FOUND, TOO_LARGE, NO_MEMORY
};

enum delta_result_type {
    OK, NON_NUMERIC, EOM, DELTA_ITEM_NOT_FOUND, DELTA_ITEM_CAS_MISMATCH
};

/** Time relative to server start. Smaller than time_t on 64-bit systems. */
// TODO: Move to sub-header. needed in logger.h
//typedef unsigned int rel_time_t;

/** Use X macros to avoid iterating over the stats fields during reset and
 * aggregation. No longer have to add new stats in 3+ places.
 */

#define SLAB_STATS_FIELDS \
    X(set_cmds) \
    X(get_hits) \
    X(touch_hits) \
    X(delete_hits) \
    X(cas_hits) \
    X(cas_badval) \
    X(incr_hits) \
    X(decr_hits)

/** Stats stored per slab (and per thread). */
struct slab_stats {
#define X(name) uint64_t    name;
    SLAB_STATS_FIELDS
#undef X
};

#define THREAD_STATS_FIELDS \
    X(get_cmds) \
    X(get_misses) \
    X(get_expired) \
    X(get_flushed) \
    X(touch_cmds) \
    X(touch_misses) \
    X(delete_misses) \
    X(incr_misses) \
    X(decr_misses) \
    X(cas_misses) \
    X(meta_cmds) \
    X(bytes_read) \
    X(bytes_written) \
    X(flush_cmds) \
    X(conn_yields) /* # of yields for connections (-R option)*/ \
    X(auth_cmds) \
    X(auth_errors) \
    X(idle_kicks) /* idle connections killed */ \
    X(response_obj_oom) \
    X(response_obj_count) \
    X(response_obj_bytes) \
    X(read_buf_oom) \
    X(store_too_large) \
    X(store_no_memory)

#ifdef EXTSTORE
#define EXTSTORE_THREAD_STATS_FIELDS \
    X(get_extstore) \
    X(get_aborted_extstore) \
    X(get_oom_extstore) \
    X(recache_from_extstore) \
    X(miss_from_extstore) \
    X(badcrc_from_extstore)
#endif

#ifdef PROXY
#define PROXY_THREAD_STATS_FIELDS \
    X(proxy_conn_requests) \
    X(proxy_conn_errors) \
    X(proxy_conn_oom)
#endif

/**
 * Stats stored per-thread.
 */
struct thread_stats {
    pthread_mutex_t   mutex;
#define X(name) uint64_t    name;
    THREAD_STATS_FIELDS
#ifdef EXTSTORE
    EXTSTORE_THREAD_STATS_FIELDS
#endif
#ifdef PROXY
    PROXY_THREAD_STATS_FIELDS
    int64_t proxy_req_active;
#endif
#undef X
    struct slab_stats slab_stats[MAX_NUMBER_OF_SLAB_CLASSES];
    uint64_t lru_hits[POWER_LARGEST];
    uint64_t read_buf_count;
    uint64_t read_buf_bytes;
    uint64_t read_buf_bytes_free;
};

/**
 * Global stats. Only resettable stats should go into this structure.
 */
struct stats {
    uint64_t      total_items;
    uint64_t      total_conns;
    uint64_t      rejected_conns;
    uint64_t      malloc_fails;
    uint64_t      listen_disabled_num;
    uint64_t      slabs_moved;       /* times slabs were moved around */
    uint64_t      slab_reassign_rescues; /* items rescued during slab move */
    uint64_t      slab_reassign_inline_reclaim; /* valid items lost during slab move */
    uint64_t      slab_reassign_chunk_rescues; /* chunked-item chunks recovered */
    uint64_t      slab_reassign_busy_items; /* valid temporarily unmovable */
    uint64_t      slab_reassign_busy_deletes; /* refcounted items killed */
    uint64_t      slab_reassign_busy_nomem; /* valid items lost during slab move */
    uint64_t      lru_crawler_starts; /* Number of item crawlers kicked off */
    uint64_t      lru_maintainer_juggles; /* number of LRU bg pokes */
    uint64_t      time_in_listen_disabled_us;  /* elapsed time in microseconds while server unable to process new connections */
    uint64_t      log_worker_dropped; /* logs dropped by worker threads */
    uint64_t      log_worker_written; /* logs written by worker threads */
    uint64_t      log_watcher_skipped; /* logs watchers missed */
    uint64_t      log_watcher_sent; /* logs sent to watcher buffers */
#ifdef EXTSTORE
    uint64_t      extstore_compact_lost; /* items lost because they were locked */
    uint64_t      extstore_compact_rescues; /* items re-written during compaction */
    uint64_t      extstore_compact_skipped; /* unhit items skipped during compaction */
    uint64_t      extstore_compact_resc_cold; /* items re-written during compaction */
    uint64_t      extstore_compact_resc_old; /* items re-written during compaction */
#endif
#ifdef TLS
    uint64_t      ssl_proto_errors; /* TLS failures during SSL_read() and SSL_write() calls */
    uint64_t      ssl_handshake_errors; /* TLS failures at accept/handshake time */
    uint64_t      ssl_new_sessions; /* successfully negotiated new (non-reused) TLS sessions */
#endif
    struct timeval maxconns_entered;  /* last time maxconns entered */
    uint64_t      unexpected_napi_ids;  /* see doc/napi_ids.txt */
    uint64_t      round_robin_fallback; /* see doc/napi_ids.txt */
};

/**
 * Global "state" stats. Reflects state that shouldn't be wiped ever.
 * Ordered for some cache line locality for commonly updated counters.
 */
struct stats_state {
    uint64_t      curr_items;
    uint64_t      curr_bytes;
    uint64_t      curr_conns;
    uint64_t      hash_bytes;       /* size used for hash tables */
    float         extstore_memory_pressure; /* when extstore might memory evict */
    unsigned int  conn_structs;
    unsigned int  reserved_fds;
    unsigned int  hash_power_level; /* Better hope it's not over 9000 */
    unsigned int  log_watchers; /* number of currently active watchers */
    bool          hash_is_expanding; /* If the hash table is being expanded */
    bool          accepting_conns;  /* whether we are currently accepting */
    bool          slab_reassign_running; /* slab reassign in progress */
    bool          lru_crawler_running; /* crawl in progress */
};

#define MAX_VERBOSITY_LEVEL 2

/* 新增配置项时，记得同步更新 process_stat_settings 的统计输出 */
/**
 * 全局配置：由命令行参数解析而来，memcached 运行期间可随时读取。
 * 涵盖了监听端口、线程数、内存上限、slab 策略、LRU 策略等核心参数。
 */
struct settings {
    size_t maxbytes;
    int maxconns;
    int port;
    int udpport;
    char *inter;
    int verbose;
    rel_time_t oldest_live; /* ignore existing items older than this */
    int evict_to_free;
    char *socketpath;   /* path to unix socket if using local socket */
    char *auth_file;    /* path to user authentication file */
    int access;  /* access mask (a la chmod) for unix domain socket */
    double factor;          /* chunk size growth factor */
    int chunk_size;
    int num_threads;        /* number of worker (without dispatcher) libevent threads to run */
    int num_threads_per_udp; /* number of worker threads serving each udp socket */
    char prefix_delimiter;  /* character that marks a key prefix (for stats) */
    int detail_enabled;     /* nonzero if we're collecting detailed stats */
    int reqs_per_event;     /* Maximum number of io to process on each
                               io-event. */
    bool use_cas;
    enum protocol binding_protocol;
    int backlog;
    int item_size_max;        /* Maximum item size */
    int slab_chunk_size_max;  /* Upper end for chunks within slab pages. */
    int slab_page_size;     /* Slab's page units. */
    volatile sig_atomic_t sig_hup;  /* a HUP signal was received but not yet handled */
    bool sasl;              /* SASL on/off */
    bool maxconns_fast;     /* Whether or not to early close connections */
    bool lru_crawler;        /* Whether or not to enable the autocrawler thread */
    bool lru_maintainer_thread; /* LRU maintainer background thread */
    bool lru_segmented;     /* Use split or flat LRU's */
    bool slab_reassign;     /* Whether or not slab reassignment is allowed */
    bool ssl_enabled; /* indicates whether SSL is enabled */
    int slab_automove;     /* Whether or not to automatically move slabs */
    unsigned int slab_automove_version; /* bump if AM config args change */
    double slab_automove_ratio; /* youngest must be within pct of oldest */
    double slab_automove_freeratio; /* % of memory to hold free as buffer */
    unsigned int slab_automove_window; /* window mover for algorithm */
    int hashpower_init;     /* Starting hash power level */
    bool shutdown_command; /* allow shutdown command */
    int tail_repair_time;   /* LRU tail refcount leak repair time */
    bool flush_enabled;     /* flush_all enabled */
    bool dump_enabled;      /* whether cachedump/metadump commands work */
    char *hash_algorithm;     /* Hash algorithm in use */
    int lru_crawler_sleep;  /* Microsecond sleep between items */
    uint32_t lru_crawler_tocrawl; /* Number of items to crawl per run */
    int hot_lru_pct; /* percentage of slab space for HOT_LRU */
    int warm_lru_pct; /* percentage of slab space for WARM_LRU */
    double hot_max_factor; /* HOT tail age relative to COLD tail */
    double warm_max_factor; /* WARM tail age relative to COLD tail */
    int crawls_persleep; /* Number of LRU crawls to run before sleeping */
    bool temp_lru; /* TTL < temporary_ttl uses TEMP_LRU */
    uint32_t temporary_ttl; /* temporary LRU threshold */
    int idle_timeout;       /* Number of seconds to let connections idle */
    unsigned int logger_watcher_buf_size; /* size of logger's per-watcher buffer */
    unsigned int logger_buf_size; /* size of per-thread logger buffer */
    unsigned int read_buf_mem_limit; /* total megabytes allowable for net buffers */
    bool drop_privileges;   /* Whether or not to drop unnecessary process privileges */
    bool watch_enabled; /* allows watch commands to be dropped */
    bool relaxed_privileges;   /* Relax process restrictions when running testapp */
    struct slab_rebal_thread *slab_rebal; /* struct for page mover thread */
#ifdef EXTSTORE
    unsigned int ext_io_threadcount; /* number of IO threads to run. */
    unsigned int ext_page_size; /* size in megabytes of storage pages. */
    unsigned int ext_item_size; /* minimum size of items to store externally */
    unsigned int ext_item_age; /* max age of tail item before storing ext. */
    unsigned int ext_low_ttl; /* remaining TTL below this uses own pages */
    unsigned int ext_recache_rate; /* counter++ % recache_rate == 0 > recache */
    unsigned int ext_wbuf_size; /* read only note for the engine */
    unsigned int ext_compact_under; /* when fewer than this many pages, compact */
    unsigned int ext_drop_under; /* when fewer than this many pages, drop COLD items */
    unsigned int ext_max_sleep; /* maximum sleep time for extstore bg threads, in us */
    double ext_max_frag; /* ideal maximum page fragmentation */
    bool ext_drop_unread; /* skip unread items during compaction */
    /* start flushing to extstore after memory below this */
    unsigned int ext_global_pool_min;
#endif
#ifdef TLS
    void *ssl_ctx; /* holds the SSL server context which has the server certificate */
    char *ssl_chain_cert; /* path to the server SSL chain certificate */
    char *ssl_key; /* path to the server key */
    int ssl_verify_mode; /* client certificate verify mode */
    int ssl_keyformat; /* key format , default is PEM */
    char *ssl_ciphers; /* list of SSL ciphers */
    char *ssl_ca_cert; /* certificate with CAs. */
    rel_time_t ssl_last_cert_refresh_time; /* time of the last server certificate refresh */
    unsigned int ssl_wbuf_size; /* size of the write buffer used by ssl_sendmsg method */
    bool ssl_session_cache; /* enable SSL server session caching */
    bool ssl_kernel_tls; /* enable server kTLS */
    int ssl_min_version; /* minimum SSL protocol version to accept */
#endif
    int num_napi_ids;   /* maximum number of NAPI IDs */
    char *memory_file;  /* warm restart memory file path */
#ifdef PROXY
    bool proxy_enabled;
    bool proxy_uring; /* if the proxy should use io_uring */
    bool proxy_memprofile; /* output detail of lua allocations */
    char *proxy_startfile; /* lua file to run when workers start */
    char *proxy_startarg; /* string argument to pass to proxy */
    void *proxy_ctx; /* proxy's state context */
#endif
#ifdef SOCK_COOKIE_ID
    uint32_t sock_cookie_id;
#endif
};

extern struct stats stats;
extern struct stats_state stats_state;
extern time_t process_started;
extern struct settings settings;

/* 表示 item 已挂入哈希表与 LRU 队列（处于“已链接”状态） */
#define ITEM_LINKED 1
/* 表示 item 带有 CAS 字段（占用 8 字节） */
#define ITEM_CAS 2

/* 临时状态：item 正被分配在 slab 中尚未链接 */
#define ITEM_SLABBED 4

/* item 在其生命周期内至少被访问（fetch）过一次 */
#define ITEM_FETCHED 8
/* 访问时在 LRU 中标记为活跃；LRU 重排时会被清除 */
#define ITEM_ACTIVE 16
/* item 的数据由链式 chunk 组成（大对象被切分存储） */
#define ITEM_CHUNKED 32
/* 当前结构本身是一个 chunk（而非完整 item 头） */
#define ITEM_CHUNK 64
/* item 的 data 主体存储于 item 之外的外部存储中 */
#define ITEM_HDR 128
/* item 带有额外的 4（或 8）字节客户端 flag */
#define ITEM_CFLAGS 256
/* item 已经发出过一个 token */
#define ITEM_TOKEN_SENT 512
/* 保留位，供将来 token 作为 2-bit 计数使用 */
#define ITEM_TOKEN_RESERVED 1024
/* item 被标记为过期/陈旧值 */
#define ITEM_STALE 2048
/* item 的 key 以二进制形式发送 */
#define ITEM_KEY_BINARY 4096

/**
 * Structure for storing items within memcached.
 */
/* memcached 中存储对象（缓存条目）的核心结构。
 * 注意其内存布局紧凑：结构体尾部之后紧跟着 key、可选的客户端 flag、
 * 可选的 CAS、以及 value 数据，以节省内存开销。 */
typedef struct _stritem {
    /* 受 LRU 锁保护：用于 LRU 双向链表的指针 */
    struct _stritem *next;
    struct _stritem *prev;
    /* 其余字段受 item 锁保护 */
    struct _stritem *h_next;    /* 哈希冲突链中的下一个 item */
    rel_time_t      time;       /* 最近一次访问时间（用于 LRU） */
    rel_time_t      exptime;    /* 过期时间（0 表示永不过期） */
    int             nbytes;     /* value 数据的字节数 */
    unsigned short  refcount;   /* 引用计数，防止被并发释放 */
    uint16_t        it_flags;   /* 见上方 ITEM_* 标志位 */
    uint8_t         slabs_clsid;/* 所在 slab class id（含高 2 位 LRU id） */
    uint8_t         nkey;       /* key 长度（含结尾 '\0' 及对齐填充） */
    /* 这个特殊的联合体避免了在不使用 CAS 时做“空间压缩”带来的类型双关问题 */
    union {
        uint64_t cas;
        char end;
    } data[];
    /* if it_flags & ITEM_CAS we have 8 bytes CAS */
    /* then null-terminated key */
    /* then " flags length\r\n" (no terminating null) */
    /* then data with terminating \r\n (no terminating null; it's binary!) */
} item;

// TODO: If we eventually want user loaded modules, we can't use an enum :(
enum crawler_run_type {
    CRAWLER_AUTOEXPIRE=0, CRAWLER_EXPIRED, CRAWLER_METADUMP, CRAWLER_MGDUMP
};

typedef struct {
    struct _stritem *next;
    struct _stritem *prev;
    struct _stritem *h_next;    /* hash chain next */
    rel_time_t      time;       /* least recent access */
    rel_time_t      exptime;    /* expire time */
    int             nbytes;     /* size of data */
    unsigned short  refcount;
    uint16_t        it_flags;   /* ITEM_* above */
    uint8_t         slabs_clsid;/* which slab class we're in */
    uint8_t         nkey;       /* key length, w/terminating null and padding */
    uint32_t        remaining;  /* Max keys to crawl per slab per invocation */
    uint64_t        reclaimed;  /* items reclaimed during this crawl. */
    uint64_t        unfetched;  /* items reclaimed unfetched during this crawl. */
    uint64_t        checked;    /* items examined during this crawl. */
} crawler;

/* Header when an item is actually a chunk of another item. */
typedef struct _strchunk {
    struct _strchunk *next;     /* points within its own chain. */
    struct _strchunk *prev;     /* can potentially point to the head. */
    struct _stritem  *head;     /* always points to the owner chunk */
    int              size;      /* available chunk space in bytes */
    int              used;      /* chunk space used */
    int              nbytes;    /* used. */
    unsigned short   refcount;  /* used? */
    uint16_t         it_flags;  /* ITEM_* above. */
    uint8_t          slabs_clsid; /* Same as above. */
    uint8_t          orig_clsid; /* For obj hdr chunks slabs_clsid is fake. */
    char data[];
} item_chunk;

#ifdef NEED_ALIGN
static inline char *ITEM_schunk(item *it) {
    int offset = it->nkey + 1
        + ((it->it_flags & ITEM_CFLAGS) ? sizeof(client_flags_t) : 0)
        + ((it->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0);
    int remain = offset % 8;
    if (remain != 0) {
        offset += 8 - remain;
    }
    return ((char *) &(it->data)) + offset;
}
#else
#define ITEM_schunk(item) ((char*) &((item)->data) + (item)->nkey + 1 \
         + (((item)->it_flags & ITEM_CFLAGS) ? sizeof(client_flags_t) : 0) \
         + (((item)->it_flags & ITEM_CAS) ? sizeof(uint64_t) : 0))
#endif

#ifdef EXTSTORE
typedef struct {
    unsigned int page_version; /* from IO header */
    unsigned int offset; /* from IO header */
    unsigned short page_id; /* from IO header */
} item_hdr;
#endif

#define IO_QUEUE_COUNT 3

#define IO_QUEUE_NONE 0
#define IO_QUEUE_EXTSTORE 1
#define IO_QUEUE_PROXY 2

#define IO_PENDING_TYPE_NONE 0
#define IO_PENDING_TYPE_BASIC 1
#define IO_PENDING_TYPE_EXTSTORE 2
#define IO_PENDING_TYPE_PROXY 3

typedef STAILQ_HEAD(iop_head_s, _io_pending_t) iop_head_t;
typedef struct _io_pending_t io_pending_t;
typedef struct io_queue_s io_queue_t;
typedef void (*io_queue_stack_cb)(io_queue_t *q);
typedef void (*io_queue_cb)(io_pending_t *pending);
// IO pending objects are created and stacked into this structure. They are
// then sent off to remote threads.
// The objects are returned one at a time to the worker threads.
typedef struct io_queue_s {
    void *ctx; // untouched ptr for specific context
    iop_head_t stack;
    io_queue_stack_cb submit_cb; // callback given a full stack of pending IO's at once.
    int type;
} io_queue_t;

struct thread_notify {
    struct event notify_event;  /* listen event for notify pipe or eventfd */
#ifdef HAVE_EVENTFD
    int notify_event_fd;        /* notify counter */
#else
    int notify_receive_fd;      /* receiving end of notify pipe */
    int notify_send_fd;         /* sending end of notify pipe */
#endif
};

typedef struct _mc_resp_bundle mc_resp_bundle;
/* 工作线程（worker thread）的核心对象：每个线程拥有独立的 libevent 事件循环，
 * 通过通知管道/eventfd 与主线程或其它线程通信，并在多线程间安全传递连接。 */
typedef struct {
    pthread_t thread_id;        /* 线程唯一 ID */
    struct event_base *base;    /* 该线程使用的 libevent 事件循环 */
    struct thread_notify n;     /* 线程间通知机制（接收新连接等） */
    struct thread_notify ion;   /* IO 对象回收通知机制 */
    pthread_mutex_t ion_lock;   /* 保护 ion_head 的互斥锁 */
    iop_head_t ion_head;        /* IO 对象回收队列 */
    int cur_sfd;                /* 记录当前客户端 fd，用于日志命令 */
    int thread_baseid;          /* 该线程的编号，用于数据偏移 */
    int conns_tosubmit;         /* 已放入 io_queue 的连接数 */
    struct thread_stats stats;  /* 该线程产生的统计数据 */
    io_queue_t io_queues[IO_QUEUE_COUNT]; /* IO 队列（外部存储/代理等） */
    struct conn_queue *ev_queue; /* 连接事件队列 */
    cache_t *rbuf_cache;        /* 固定大小的读缓冲区缓存池 */
    mc_resp_bundle *open_bundle;/* 当前打开的响应 bundle */
    cache_t *io_cache;          /* IO 对象缓存池 */
#ifdef EXTSTORE
    void *storage;              /* 外部存储系统数据对象 */
#endif
    logger *l;                  /* 该线程的日志缓冲区 */
    void *lru_bump_buf;         /* 异步 LRU 更新缓冲 */
#ifdef TLS
    char   *ssl_wbuf;           /* SSL 写缓冲 */
#endif
    int napi_id;                /* 该线程关联的 NAPI id（网络加速） */
#ifdef PROXY
    void *proxy_ctx; // 代理全局上下文
    void *L; // Lua 虚拟机
    void *proxy_hooks;
    void *proxy_user_stats;
    void *proxy_int_stats;
    void *proxy_event_thread; // worker 线程也可作为代理 IO 线程
    struct event *proxy_gc_timer; // 周期性 GC 定时器
    pthread_mutex_t proxy_limit_lock;
    int proxy_vm_extra_kb;
    int proxy_vm_last_kb;
    unsigned int proxy_vm_negative_delta;
    int proxy_vm_gcrunning;
    int proxy_vm_gcpokemem;
    int64_t proxy_active_req_limit;
    uint64_t proxy_buffer_memory_limit; // 受 limit_lock 保护
    uint64_t proxy_buffer_memory_used; // protected by limit_lock
    uint32_t proxy_rng[4]; // fast per-thread rng for lua.
    // TODO: add ctx object so we can attach to queue.
#endif
} LIBEVENT_THREAD;

/**
 * Response objects
 */
#define MC_RESP_IOVCOUNT 4
/* 单个响应对象：连接通过“响应链”向客户端回送数据。
 * 支持一次性聚合多个 iovec，减少系统调用次数，提高网络吞吐。 */
typedef struct _mc_resp {
    mc_resp_bundle *bundle; /* 指回所属的响应 bundle */
    struct _mc_resp *next; /* 响应链表中的下一个 */
    int wbytes; /* 已经从 wbuf 写出（剩余）的字节数 */
    int tosend; /* 该响应总共需要发送的字节数 */
    void *write_and_free; /** 写出完成后需要 free 的内存 */
    io_pending_t *io_pending; /* 该响应关联的异步 IO 描述符 */

    item *item; /* 关联的 item（持有引用，防止被释放） */
    struct iovec iov[MC_RESP_IOVCOUNT]; /* 内置 iovec 数组，简化网络发送 */
    int chunked_total; /* 需要发送的链式 chunk 数据总量 */
    uint8_t iovcnt; /* 实际使用的 iovec 数量 */
    uint8_t chunked_data_iov; /* 指向 chunk 数据头的 iov 下标 */

    /* 通知发送逻辑跳过该响应对象：存储引擎可异步取消已排队写入的响应 */
    bool skip;
    bool suspended; /* 等待子系统（如外部存储）返回结果 */
    bool free; /* 双重释放检测标志 */
#ifdef PROXY
    bool proxy_res; /* 正在处理代理返回的缓冲区 */
#endif
    // UDP 相关字段，从客户端请求中拷贝而来
    uint16_t    request_id; /* 入站 UDP 请求 ID */
    uint16_t    udp_sequence; /* 发送结果时的包序号 */
    uint16_t    udp_total; /* 序列中的总包数 */
    struct sockaddr_in6 request_addr; /* UDP：请求来源地址 */
    socklen_t request_addr_size;

    char wbuf[WRITE_BUFFER_SIZE]; /* 内置写缓冲区 */
} mc_resp;

/* 一个 bundle 中最多能放多少个响应对象（按内存大小计算） */
#define MAX_RESP_PER_BUNDLE ((READ_BUFFER_SIZE - sizeof(mc_resp_bundle)) / sizeof(mc_resp))
/* 响应对象并非单独 malloc，而是成组（bundle）批量分配，减少内存碎片 */
struct _mc_resp_bundle {
    uint8_t refcount;
    uint8_t next_check; /* 分配时下一个待检查的对象下标 */
    LIBEVENT_THREAD *thread;
    struct _mc_resp_bundle *next;
    struct _mc_resp_bundle *prev;
    mc_resp r[]; /* 柔性数组，存放一组响应对象 */
};

typedef struct conn conn;

struct _io_pending_t {
    uint8_t io_queue_type; // which queue this entered on
    uint8_t io_sub_type; // sub-type. FIXME: can optimize out later.
    uint8_t payload;
    LIBEVENT_THREAD *thread;
    conn *c;
    mc_resp *resp; // associated response object
    io_queue_cb return_cb; // called on worker thread.
    io_queue_cb finalize_cb; // called back on the worker thread.
    STAILQ_ENTRY(_io_pending_t) iop_next; // queue chain.
    char data[120];
};

/**
 * memcached 客户端连接的核心结构，每个连接对应一个状态机。
 * 通过 libevent 监听可读/可写事件，按 conn_states 状态逐步处理命令。
 */
struct conn {
    sasl_conn_t *sasl_conn; /* SASL 认证上下文 */
    int    sfd;             /* 客户端套接字文件描述符 */
    bool sasl_started;      /* 是否已开始 SASL 握手 */
    bool authenticated;     /* 是否已完成认证 */
    bool set_stale;         /* 是否将写入标记为陈旧 */
    bool mset_res;          /* 使用 mset 格式返回结果码 */
    bool close_after_write; /* 写完响应后关闭连接 */
    bool rbuf_malloced;     /* 读缓冲区是否为 ascii mget 临时 malloc 的 */
    bool item_malloced;     /* conn_nread 阶段的 item 是否为临时 malloc */
    uint8_t ssl_enabled;    /* 是否启用 SSL/TLS */
    void    *ssl;           /* SSL/TLS 会话对象 */
#ifdef TLS
    char   *ssl_wbuf;       /* SSL 写缓冲区 */
#endif
    enum conn_states  state;    /* 当前连接状态 */
    enum bin_substates substate;/* 二进制协议子状态 */
    rel_time_t last_cmd_time;   /* 最后一条命令的时间，用于超时判断 */
    struct event event;         /* libevent 事件对象 */
    short  ev_flags;            /* 关注的事件标志（读/写） */
    short  which;   /** 刚刚触发的事件类型 */

    char   *rbuf;   /** 读取命令用的输入缓冲区 */
    char   *rcurr;  /** 已解析部分之后的当前读取位置 */
    int    rsize;   /** rbuf 的总分配大小 */
    int    rbytes;  /** 从 rcurr 起尚未解析的字节数 */

    mc_resp *resp;       /* 响应链表尾 */
    mc_resp *resp_head;  /* 当前响应用例的头 */
    char   *ritem;  /** 读取 item 的 value 时，数据写入此临时缓冲区 */
    int    rlbytes;      /* 还需读取的 value 字节数 */

    /**
     * set/add/replace 命令读取完命令行但尚未读完 value 数据时，
     * 会先创建 item 结构放在此处；value 直接读入 ITEM_data(item)，避免二次拷贝。
     */
    void   *item;     /* set/add/replace 等命令使用的临时 item */

    /* swallow 状态所需的数据 */
    int    sbytes;    /* 需要丢弃（swallow）的字节数 */

    int resps_suspended; /* 被挂起的响应数量（异步 IO 等待时） */
#ifdef PROXY
    void *proxy_rctx; /* 当前代理请求上下文 */
#endif
#ifdef EXTSTORE
    unsigned int recache_counter; /* 重新缓存计数 */
#endif
    enum protocol protocol;   /* 该连接使用的协议（ascii/binary） */
    enum network_transport transport; /* 传输方式（tcp/udp/unix socket） */
    enum close_reasons close_reason; /* 进入 conn_closing 的原因 */

    /* UDP 客户端相关数据 */
    int    request_id; /* 入站 UDP 请求 ID */
    struct sockaddr_in6 request_addr; /* 最近一次请求的来源地址 */
    socklen_t request_addr_size;

    bool   noreply;   /* 为 true 时不发送回复（命令带 noreply 选项） */
    /* 当前 stats 命令的累积缓冲 */
    struct {
        char *buffer;
        size_t size;
        size_t offset;
    } stats;

    /* 二进制协议相关字段 */
    protocol_binary_request_header binary_header; /* 存放二进制协议头 */
    uint64_t cas; /* 需要返回的 cas 值 */
    uint64_t tag; /* 监听器套接字标签 */
    short cmd;    /* 当前正在处理的命令 */
    int opaque;   /* 二进制协议 opaque 透传字段 */
    int keylen;   /* 当前 key 长度 */
    conn   *next;     /* 用于把所有 conn 串成链表 */
    LIBEVENT_THREAD *thread; /* 服务该连接的线程对象 */
    int (*try_read_command)(conn *c); /* 顶层输入解析函数指针 */
    ssize_t (*read)(conn  *c, void *buf, size_t count);  /* 读函数指针 */
    ssize_t (*sendmsg)(conn *c, struct msghdr *msg, int flags); /* 发送函数 */
    ssize_t (*write)(conn *c, void *buf, size_t count);  /* 写函数指针 */
};

/* array of conn structures, indexed by file descriptor */
extern conn **conns;

/* current time of day (updated periodically) */
extern volatile rel_time_t current_time;

#ifdef MEMCACHED_DEBUG
extern volatile bool is_paused;
extern volatile int64_t delta;
#endif

#ifdef EXTSTORE
extern void *ext_storage;
#endif
/*
 * Functions
 */
void verify_default(const char* param, bool condition);
void do_accept_new_conns(const bool do_accept);
enum delta_result_type do_add_delta(LIBEVENT_THREAD *t, const char *key,
                                    const size_t nkey, const bool incr,
                                    const int64_t delta, char *buf,
                                    uint64_t *cas, const uint32_t hv,
                                    item **it_ret);
enum store_item_type do_store_item(item *item, int comm, LIBEVENT_THREAD *t, const uint32_t hv, int *nbytes, uint64_t *cas, const uint64_t cas_in, bool cas_stale);
void thread_io_queue_add(LIBEVENT_THREAD *t, int type, void *ctx, io_queue_stack_cb cb);
io_queue_t *thread_io_queue_get(LIBEVENT_THREAD *t, int type);
void thread_io_queue_submit(LIBEVENT_THREAD *t);
void conn_io_queue_return(io_pending_t *io);
#define conn_resp_suspend(c, resp) \
    do { \
        resp->suspended = true; \
        c->resps_suspended++; \
    } while (0)
#define conn_resp_unsuspend(c, resp) \
    do { \
        resp->suspended = false; \
        c->resps_suspended--; \
        assert(c->resps_suspended >= 0); \
        if (c->resps_suspended == 0) { \
            conn_worker_readd(c); \
        } \
    } while (0)

conn *conn_new(const int sfd, const enum conn_states init_state, const int event_flags, const int read_buffer_size,
    enum network_transport transport, struct event_base *base, void *ssl, uint64_t conntag, enum protocol bproto);

void conn_worker_readd(conn *c);
extern int daemonize(int nochdir, int noclose);

#define mutex_lock(x) pthread_mutex_lock(x)
#define mutex_trylock(x) pthread_mutex_trylock(x)
#define mutex_unlock(x) pthread_mutex_unlock(x)

#include "stats_prefix.h"
#include "slabs.h"
#include "assoc.h"
#include "items.h"
#include "crawler.h"
#include "trace.h"
#include "hash.h"

/*
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */
void memcached_thread_init(int nthreads, void *arg);
void redispatch_conn(conn *c);
void timeout_conn(conn *c);
#ifdef PROXY
void proxy_reload_notify(LIBEVENT_THREAD *t);
#endif
void return_io_pending(io_pending_t *io);
void dispatch_conn_new(int sfd, enum conn_states init_state, int event_flags, int read_buffer_size,
    enum network_transport transport, void *ssl, uint64_t conntag, enum protocol bproto);
void sidethread_conn_close(conn *c);

/* Lock wrappers for cache functions that are called from main loop. */
enum delta_result_type add_delta(LIBEVENT_THREAD *t, const char *key,
                                 const size_t nkey, bool incr,
                                 const int64_t delta, char *buf,
                                 uint64_t *cas);
void accept_new_conns(const bool do_accept);
void  conn_close_idle(conn *c);
void  conn_close_all(void);
item *item_alloc(const char *key, size_t nkey, client_flags_t flags, rel_time_t exptime, int nbytes);
#define DO_UPDATE true
#define DONT_UPDATE false
item *item_get(const char *key, const size_t nkey, LIBEVENT_THREAD *t, const bool do_update);
item *item_get_locked(const char *key, const size_t nkey, LIBEVENT_THREAD *t, const bool do_update, uint32_t *hv);
item *item_touch(const char *key, const size_t nkey, uint32_t exptime, LIBEVENT_THREAD *t);
int   item_link(item *it);
void  item_remove(item *it);
int   item_replace(item *it, item *new_it, const uint32_t hv, const uint64_t cas_in);
void  item_unlink(item *it);

void item_lock(uint32_t hv);
void *item_trylock(uint32_t hv);
void item_trylock_unlock(void *arg);
void item_unlock(uint32_t hv);
void pause_threads(enum pause_thread_types type);
void stop_threads(void);
int stop_conn_timeout_thread(void);
#define refcount_incr(it) ++(it->refcount)
#define refcount_decr(it) --(it->refcount)
void STATS_LOCK(void);
void STATS_UNLOCK(void);
#define THR_STATS_LOCK(t) pthread_mutex_lock(&t->stats.mutex)
#define THR_STATS_UNLOCK(t) pthread_mutex_unlock(&t->stats.mutex)
void threadlocal_stats_reset(void);
void threadlocal_stats_aggregate(struct thread_stats *stats);
void slab_stats_aggregate(struct thread_stats *stats, struct slab_stats *out);
void thread_setname(pthread_t thread, const char *name);
LIBEVENT_THREAD *get_worker_thread(int id);

/* Stat processing functions */
void append_stat(const char *name, ADD_STAT add_stats, conn *c,
                 const char *fmt, ...);

enum store_item_type store_item(item *item, int comm, LIBEVENT_THREAD *t, int *nbytes, uint64_t *cas, const uint64_t cas_in, bool cas_stale);

/* Protocol related code */
void out_string(conn *c, const char *str);
#define REALTIME_MAXDELTA 60*60*24*30
/* Negative exptimes can underflow and end up immortal. realtime() will
   immediately expire values that are greater than REALTIME_MAXDELTA, but less
   than process_started, so lets aim for that. */
#define EXPTIME_TO_POSITIVE_TIME(exptime) (exptime < 0) ? \
        REALTIME_MAXDELTA + 1 : exptime
rel_time_t realtime(const time_t exptime);
item* limited_get(const char *key, size_t nkey, LIBEVENT_THREAD *t, uint32_t exptime, bool should_touch, bool do_update, bool *overflow);
item* limited_get_locked(const char *key, size_t nkey, LIBEVENT_THREAD *t, bool do_update, uint32_t *hv, bool *overflow);
// Read/Response object handlers.
void resp_reset(mc_resp *resp);
void resp_add_iov(mc_resp *resp, const void *buf, int len);
void resp_add_chunked_iov(mc_resp *resp, const void *buf, int len);
bool resp_start(conn *c);
mc_resp *resp_start_unlinked(conn *c);
mc_resp* resp_finish(conn *c, mc_resp *resp);
void resp_free(LIBEVENT_THREAD *th, mc_resp *resp);
bool resp_has_stack(conn *c);
bool rbuf_switch_to_malloc(conn *c);
void conn_release_items(conn *c);
void conn_set_state(conn *c, enum conn_states state);
void out_of_memory(conn *c, char *ascii_error);
void out_errstring(conn *c, const char *str);
void write_and_free(conn *c, char *buf, int bytes);
void server_stats(ADD_STAT add_stats, void *c);
void append_stats(const char *key, const uint16_t klen,
                  const char *val, const uint32_t vlen,
                  const void *cookie);
/** Return a datum for stats in binary protocol */
bool get_stats(const char *stat_type, int nkey, ADD_STAT add_stats, void *c);
void stats_reset(void);
void process_stat_settings(ADD_STAT add_stats, void *c);
void process_stats_conns(ADD_STAT add_stats, void *c);

#if HAVE_DROP_PRIVILEGES
extern void setup_privilege_violations_handler(void);
extern void drop_privileges(void);
#else
#define setup_privilege_violations_handler()
#define drop_privileges()
#endif

#if HAVE_DROP_WORKER_PRIVILEGES
extern void drop_worker_privileges(void);
#else
#define drop_worker_privileges()
#endif

/* If supported, give compiler hints for branch prediction. */
#if !defined(__GNUC__) || (__GNUC__ == 2 && __GNUC_MINOR__ < 96)
#define __builtin_expect(x, expected_value) (x)
#endif

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#endif /* MEMCACHED_H */
