/* slabs 内存分配器：memcached 的核心内存管理模块。
 * 内存被切分为固定大小的“页（page，默认 1MB）”，每页再切分为等大的
 * “块（chunk）”。相同 chunk 大小的页归到同一“slab class”。
 * 用分级（slab class）的方式缓解传统 malloc 的碎片问题。 */
#ifndef SLABS_H
#define SLABS_H

/** 初始化 slab 子系统。
 *  limit  : 可分配内存上限（字节），0 表示不限（实际受系统限制）
 *  factor : 增长因子，每个 slab class 的 chunk 大小为上一个的 factor 倍
 *  prealloc: 是否启动时一次性预分配全部内存
 *  slab_sizes: 可选的自定义各 class 的 chunk 大小数组
 *  mem_base_external: 外部指定的内存基址（用于重启复用内存）
 *  reuse_mem: 是否复用已有内存（warm restart）
 */
void slabs_init(const size_t limit, const double factor, const bool prealloc, const uint32_t *slab_sizes, void *mem_base_external, bool reuse_mem);

/** Call only during init. Pre-allocates all available memory */
void slabs_prefill_global(void);

/**
 * 根据对象大小，返回应使用的 slab class id。
 * 返回 0 表示错误：对象过大，无法存储（超过 item_size_max）。
 */
unsigned int slabs_clsid(const size_t size);
unsigned int slabs_size(const int clsid);

/** 分配指定 class 的对象。失败返回 NULL。
 *  SLABS_ALLOC_NO_NEWPAGE 标志：内存不足时不允许新建页，直接失败 */
#define SLABS_ALLOC_NO_NEWPAGE 1
void *slabs_alloc(unsigned int id, unsigned int flags);

/** 释放之前分配的对象（归还到所在 class 的空闲链表） */
void slabs_free(void *ptr, unsigned int id);

/** 动态调整全局内存上限（扩/缩容） */
bool slabs_adjust_mem_limit(size_t new_mem_limit);

typedef struct {
    unsigned int chunks_per_page;
    unsigned int chunk_size;
    long int free_chunks;
    long int total_pages;
} slab_stats_automove;
void fill_slab_stats_automove(slab_stats_automove *am);
unsigned int global_page_pool_size(bool *mem_flag);

/** Fill buffer with stats */ /*@null@*/
void slabs_stats(ADD_STAT add_stats, void *c);

/* Hints as to freespace in slab class */
unsigned int slabs_available_chunks(unsigned int id, bool *mem_flag, unsigned int *chunks_perslab);

void slabs_mlock(void);
void slabs_munlock(void);

/* utilities for page moving */
void *slabs_peek_page(const unsigned int id, uint32_t *size, uint32_t *perslab);
void do_slabs_unlink_free_chunk(const unsigned int id, item *it);
void slabs_finalize_page_move(const unsigned int sid, const unsigned int did, void *page);
int slabs_pick_any_for_reassign(const unsigned int did);
int slabs_page_count(const unsigned int id);

typedef int (*slabs_cb)(void *arg);
int slabs_locked_callback(slabs_cb cb, void *arg);
int slabs_grow_slab_list(const unsigned int id);

/* Fixup for restartable code. */
unsigned int slabs_fixup(char *chunk, const int border);

#endif
