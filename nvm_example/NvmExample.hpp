#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#ifdef USE_LIBPMEM
#include <libpmem.h>
#endif

#include <cstdio>
#include <cstring>
#include <include/db.hpp>
#include <mutex>
#include <string>
#include <unordered_map>

class LogAppender {
public:
    LogAppender(const char* file_name, size_t size);
    std::pair<Slice, Slice> Append(const Slice& key, const Slice& val);
    void Recovery(std::unordered_map<std::string, Slice>& hash_map);
    ~LogAppender();

    class RecoveryHelper {
    public:
        RecoveryHelper(char* pmem_base, uint64_t* end_off);
        std::pair<Slice, Slice> Next();

    private:
        void _get_slice(Slice& slice);
        uint64_t* _end_off;
        char* _pmem_base;
        uint64_t _sequence;
        uint64_t _current;
    };

private:
    Slice _push_back(const Slice& key);
    void _persist(void* addr, uint32_t len);
    union {
        uint64_t* sequence;
        char* pmem_base;
    } _pmem;
    size_t _mapped_len;
#ifdef USE_LIBPMEM
    int _is_pmem;
#endif
    uint64_t _end_off;
};

class NvmExample : DB {
public:
    const static size_t SIZE = 0x1000000;

    static Status CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file = nullptr);

    NvmExample(const std::string& name);
    Status Get(const Slice& key, std::string* value) override;
    Status Set(const Slice& key, const Slice& value) override;
    ~NvmExample() override;

private:
    LogAppender logger;
    std::mutex mut;
    std::unordered_map<std::string, Slice> hash_map;
};

#endif
