#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include "include/db.hpp"
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <libpmem.h>
#include <string>
#include <mutex>
#include <vector>
#include <atomic>
#include <list>
#include <iostream>
#include <unordered_map>
#include <bitset>
#include <byteswap.h>
#include <cstdlib>
#include <ctime>
#include <cmath>
#include <stack>
#include <sys/stat.h>
#include <queue>
#include <ctime>
#include "cpuinfo.hpp"
#include <unistd.h>
#include <ios>
#include <fstream>
using namespace std;
// #define TEST

#define TO_UINT32(buffer) (*(uint32_t*)(buffer))
#define TO_UINT16(buffer) (*(uint16_t*)(buffer))
#define TO_UINT8(buffer)  (*(uint8_t*)(buffer))
const uint64_t HASH_TABLE_LENGTH = 873741881;                 
const uint32_t KV_CACHE_LENGTH = 673499;
const uint64_t KEY_CACHE_TABLE_LENGTH = 97774177;  
#define SLICE_SIZE (32)
#define KEY_ENTRY_SIZE (23)
#define WKEY_SIZE (16)
#define LEN_SIZE (2)
#define VAL_MAX_SIZE (1024)
#define QUEUE_NUM (30)
#define FLAG_SZ (4)
const uint8_t INDEX_OFFSET = 4;
const uint64_t FILE_NUM = 128 * 6;
const uint64_t MAX_KV_SIZE = VAL_MAX_SIZE + KEY_ENTRY_SIZE;
const uint64_t TOTAL_FILE_SIZE = 68719476736;   //64G
const uint64_t SMALL_FILE_SIZE = TOTAL_FILE_SIZE / FILE_NUM;
const uint64_t SLICE_NUM = TOTAL_FILE_SIZE / SLICE_SIZE;
const uint64_t SMALL_SLICE_NUM = SLICE_NUM / FILE_NUM;
const uint8_t THREAD_NUM = 17;
const uint32_t KE_NUM_PER_THREAD = KEY_CACHE_TABLE_LENGTH / THREAD_NUM;
const uint32_t ZERO_FALG = 0;

typedef queue<uint32_t> Space_Slice_Queue;
typedef stack<uint32_t> Space_Slice_Stack;

// 用于KV cache （一级缓存）
struct KVEntry {
    char key[16];
    uint16_t size;
    char val[1024];
    bool used;
};

// 用于KeyList （二级缓存）
struct KeyEntry {
    char _key[16];
    uint16_t _size;         // value大小
    uint32_t _val_offset;   // value在文件中位置
    uint8_t _block_num;     // KV及元数据占多少个单位回收块
    uint32_t _cnt;          // 用于原子性恢复的计数
};

// 回收空间的数据结构
struct SpaceInfo {
    uint32_t slice_offset;  // 回收空间在文件中的位置
    uint8_t block_num;      // 回收空间占多少个单位回收块
};

// 自旋锁
class spin_mutex {
    std::atomic_flag flag = ATOMIC_FLAG_INIT;
public:
    spin_mutex() = default;
    spin_mutex(const spin_mutex&) = delete;
    spin_mutex& operator= (const spin_mutex&) = delete;
    void lock() {
        while(flag.test_and_set(std::memory_order_acquire));
    }
    void unlock() {
        flag.clear(std::memory_order_release);
    }
};

// 一级cache，存储67w个完整KV
// get时会添加KV到cache中
// set时若该key已存在于cache，则更新，否则不更新
class Cache {
private:
    KVEntry kv[KV_CACHE_LENGTH];
public:
    void set(const Slice& key, const char* buffer, uint16_t val_size) {
        uint32_t pos = TO_UINT32(key.data()) % KV_CACHE_LENGTH;
        memcpy(kv[pos].key, key.data(), WKEY_SIZE);
        memcpy(kv[pos].val, buffer, val_size);
        kv[pos].size = val_size;
        kv[pos].used = true;
    }
    // 若该位used flag为F，则直接返回NotFound，避免了一次memcmp
    Status get(const Slice& key, string* value) {
        uint32_t pos = TO_UINT32(key.data()) % KV_CACHE_LENGTH;
        if (kv[pos].used == false) return NotFound;
        if (memcmp(key.data(), kv[pos].key, WKEY_SIZE) == 0) {
            *value = string(kv[pos].val, kv[pos].size);
            return Ok;
        }
        return NotFound;
    }
    void update(const Slice& key, const Slice& value) {
        uint32_t pos = TO_UINT32(key.data()) % KV_CACHE_LENGTH;
        if (kv[pos].used == false) return;
        memcpy(kv[pos].key, key.data(), WKEY_SIZE);
        memcpy(kv[pos].val, value.data(), value.size());
        kv[pos].size = value.size();
        kv[pos].used = true;
    }
    Cache() {
        for (uint32_t i = 0; i < KV_CACHE_LENGTH; ++i) {
            kv[i].used = false;
        }
    }
};

// 二级cache，存储约1亿个Key及其对应offset、val_sz、block_num
// threadLocal
class KeyList {
private:
    KeyEntry ke[KEY_CACHE_TABLE_LENGTH];
    uint32_t ends[THREAD_NUM];
public:
    KeyList(){
        for (int i = 0; i < THREAD_NUM; ++ i) {
            ends[i] = i * KE_NUM_PER_THREAD;
        }
    }
    uint32_t add(int thread_id, char* key, uint16_t size, int block_num, uint32_t offset) {
        if (ends[thread_id] == (thread_id + 1) * KE_NUM_PER_THREAD) return 0;
        uint32_t pos = ++ ends[thread_id];
        memcpy(ke[pos]._key, key, WKEY_SIZE);
        ke[pos]._block_num = block_num;
        ke[pos]._size = size;
        ke[pos]._val_offset = offset;
        ke[pos]._cnt = 1;
        return pos;
    }
    void update(uint32_t pos, uint16_t size, int block_num, uint32_t offset) {
        ke[pos]._block_num = block_num;
        ke[pos]._size = size;
        ke[pos]._val_offset = offset;
        ++ ke[pos]._cnt;
    }
    KeyEntry& get(uint32_t pos) {
        return ke[pos];
    }
};

class NvmEngine : DB {
public:
    /**
     * @param 
     * name: file in AEP(exist)
     * dbptr: pointer of db object
     *
     */
    static Status CreateOrOpen(const std::string& name, DB** dbptr, FILE *_log_file = nullptr);
    Status Get(const Slice& key, std::string* value);
    Status Set(const Slice& key, const Slice& value);
    NvmEngine(const std::string& path, FILE *_log_file);
    ~NvmEngine();

private:
    KeyList kl;
    Cache cache;
    Space_Slice_Stack stacks[THREAD_NUM][QUEUE_NUM];
    FILE* log_file;
    spin_mutex lock_mutex[FILE_NUM];
    uint32_t offset_hash[HASH_TABLE_LENGTH];
    bool in_list[HASH_TABLE_LENGTH];
    char* key_addr;
    uint64_t file_end_offset[FILE_NUM];
    uint32_t get_block_offset(int block_num,uint8_t &);
    int get_block_num(uint32_t size);

    #ifdef TEST
    void process_mem_usage();
    #endif
};
#endif
