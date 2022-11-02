// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#ifndef STARROCKS_STATE_CACHE_H
#define STARROCKS_STATE_CACHE_H

#include <memory>

#include "exec/streaming/streaming_fdw.h"
#include "util/lru_cache.h"

namespace starrocks::streaming {
class StateCache;
using StateCacheRawPtr = StateCache*;
using StateCachePtr = std::shared_ptr<StateCache>;
using StateCacheUtr = std::unique_ptr<StateCache>;

/**
 * StateCache is used for keeping changes between the transactions(or barriers) and decrease IO interacts with Storage
 * layer:
 * - For each transaction(or barrier), StateTable's data cannot be read before committed, so keep state changes into
 *   StateCache to be read as soon as possible;
 * - StateCache can cache the changes of state for the specific keys to avoid IOs with Storage.
 *
 *  While normal LRUCache evicts data according `capacity` dynamically, StateCache cannot evict the cache data
 *  during the transaction(or barrier) before flushing, otherwise may cause data lose for StateTable.
 */
class StateCache : LRUCache {
public:
    StateCache() {}
    ~StateCache() = default;

    bool need_evict(size_t charge) override {
        return false;
    }

    void update_epoch(Epoch epoch) {
        std::lock_guard l(_mutex);
        this->_cur_epoch = epoch;
    }

    int evict_by_epoch(Epoch epoch) {
        std::vector<LRUHandle*> last_ref_list;
        {
            std::lock_guard l(_mutex);
            while (_lru.next != &_lru) {
                LRUHandle* old = _lru.next;
                if (old->epoch >= epoch) {
                    continue;
                }
                DCHECK(old->in_cache);
                DCHECK(old->refs == 1); // LRU list contains elements which may be evicted
                _lru_remove(old);
                _table.remove(old->key(), old->hash);
                old->in_cache = false;
                _unref(old);
                _usage -= old->charge;
                last_ref_list.push_back(old);
            }
        }
        for (auto entry : last_ref_list) {
            entry->free();
        }
        return last_ref_list.size();
    }

    Cache::Handle* insert(const CacheKey& key, void* value, size_t charge,
                          void (*deleter)(const CacheKey& key, void* value), CachePriority priority) {
        const uint32_t hash = _hash_slice(key);
        return LRUCache::insert(key, hash, value, charge, deleter, priority);
    }

    Cache::Handle* lookup(const CacheKey& key) {
        const uint32_t hash = _hash_slice(key);
        return LRUCache::lookup(key, hash);
    }

    void* value(Cache::Handle* handle) {
        return reinterpret_cast<LRUHandle*>(handle)->value;
    }
private:
    inline uint32_t _hash_slice(const CacheKey& s) {
        return s.hash(s.data(), s.size(), 0);
    }
};

}

#endif //STARROCKS_STATE_CACHE_H
