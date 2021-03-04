#include "NvmExample.hpp"
#ifdef USE_LIBPMEM
#include <libpmem.h>
#endif

#include <sys/mman.h>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    return NvmExample::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

LogAppender::RecoveryHelper::RecoveryHelper(char* pmem_base, uint64_t* end_off)
    : _end_off(end_off),
      _pmem_base(pmem_base),
      _sequence(*(uint64_t*)pmem_base),
      _current(0) {
    *_end_off = sizeof(uint64_t);
}

std::pair<Slice, Slice> LogAppender::RecoveryHelper::Next() {
    Slice key, val;
    if (_current < _sequence) {
        _current += 1;
        _get_slice(key);
        _get_slice(val);
    }
    return std::make_pair(key, val);
}

void LogAppender::RecoveryHelper::_get_slice(Slice& slice) {
    char* pmem_addr = _pmem_base + *_end_off;
    slice.size() = *(uint64_t*)pmem_addr;
    pmem_addr += sizeof(uint64_t);
    slice.data() = pmem_addr;
    pmem_addr += slice.size();
    *_end_off = pmem_addr - _pmem_base;
}

LogAppender::LogAppender(const char* file_name, size_t size) {
#ifdef USE_LIBPMEM
    if ((_pmem.pmem_base = (char*)pmem_map_file(file_name, size,
                                                PMEM_FILE_CREATE,
                                                0666, &_mapped_len,
                                                &_is_pmem)) == NULL) {
        perror("Pmem map file failed");
        exit(1);
    }
#else
    if((_pmem.pmem_base = (char*)mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED, 0, 0)) == NULL) {
        perror("mmap failed");
        exit(1);
    }
#endif
}

void LogAppender::Recovery(std::unordered_map<std::string, Slice>& hash_map) {
    RecoveryHelper helper(_pmem.pmem_base, &_end_off);
    while (true) {
        auto kv = helper.Next();
        if (kv.first.data() == nullptr)
            break;
        hash_map[kv.first.to_string()] = kv.second;
    }
}

std::pair<Slice, Slice> LogAppender::Append(const Slice& key, const Slice& val) {
    Slice nvm_key = _push_back(key);
    Slice nvm_val = _push_back(val);
    (*_pmem.sequence)++;
    return std::make_pair(nvm_key, nvm_val);
}

LogAppender::~LogAppender() {

#ifdef USE_LIBPMEM
    pmem_unmap(_pmem.pmem_base, _mapped_len);
#else
    munmap(_pmem.pmem_base, _mapped_len);
#endif
}

void LogAppender::_persist(void* addr, uint32_t len) {
#ifdef USE_LIBPMEM
    if (_is_pmem)
        pmem_persist(addr, len);
    else
        pmem_msync(addr, len);
#endif
}

Slice LogAppender::_push_back(const Slice& slice) {
    char* ptr = _pmem.pmem_base + _end_off;
    *(uint64_t*)ptr = slice.size();
    _persist(ptr, sizeof(uint64_t));
    ptr += sizeof(uint64_t);
    Slice ret(ptr, slice.size());
    memcpy(ptr, slice.data(), slice.size());
    _persist(ptr, slice.size());
    _end_off = _end_off + sizeof(uint64_t) + slice.size();
    return ret;
}

NvmExample::NvmExample(const std::string& name)
    : logger(name.c_str(), SIZE) {
    logger.Recovery(hash_map);
}

Status NvmExample::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    NvmExample* db = new NvmExample(name);
    *dbptr = db;
    return Ok;
}

Status NvmExample::Get(const Slice& key, std::string* value) {
    std::lock_guard<std::mutex> lock(mut);
    auto kv = hash_map.find(key.to_string());
    if (kv == hash_map.end()) {
        return NotFound;
    }
    *value = kv->second.to_string();
    return Ok;
}

Status NvmExample::Set(const Slice& key, const Slice& value) {
    std::lock_guard<std::mutex> lock(mut);
    auto kv = logger.Append(key, value);
    hash_map[kv.first.to_string()] = kv.second;
    return Ok;
}

NvmExample::~NvmExample() {}
