/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "memcached.h"
#include "jenkins_hash.h"
#include "murmur3_hash.h"
#define XXH_INLINE_ALL // modifier for xxh3's include below
#include "xxhash.h"

/* 全局哈希函数指针：所有 key 的散列都统一通过它计算 */
hash_func hash;

static uint32_t XXH3_hash(const void *key, size_t length) {
    return (uint32_t)XXH3_64bits(key, length);
}

/* 根据配置选择并安装哈希算法。memcached 内置三种实现：
 *   - jenkins ：经典 Jenkins one-at-a-time 哈希
 *   - murmur3 ：MurmurHash3（32 位），速度与分布俱佳（默认）
 *   - xxh3    ：XXH3，较新的高速哈希
 * 选择后记录到 settings.hash_algorithm 供统计/展示。 */
int hash_init(enum hashfunc_type type) {
    switch(type) {
        case JENKINS_HASH:
            hash = jenkins_hash;
            settings.hash_algorithm = "jenkins";
            break;
        case MURMUR3_HASH:
            hash = MurmurHash3_x86_32;
            settings.hash_algorithm = "murmur3";
            break;
        case XXH3_HASH:
            hash = XXH3_hash;
            settings.hash_algorithm = "xxh3";
            break;
        default:
            return -1;
    }
    return 0;
}
