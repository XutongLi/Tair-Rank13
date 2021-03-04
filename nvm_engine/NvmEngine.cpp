#include "NvmEngine.hpp"

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    return NvmEngine::CreateOrOpen(name, dbptr, log_file);
}

DB::~DB() {}

static thread_local uint32_t hash_result;
static thread_local uint32_t offset;
static thread_local char rbuffer[KEY_ENTRY_SIZE];
static thread_local char vbuffer[VAL_MAX_SIZE + KEY_ENTRY_SIZE + FLAG_SZ];
static thread_local uint64_t final_old_offset;
static thread_local uint64_t final_new_offset;
static thread_local uint16_t val_size;
static thread_local uint64_t idx;
static thread_local uint64_t key_hash_result;
static thread_local int thread_id;
static thread_local bool init_flag;
static atomic<int> init_thread_id;

#ifdef TEST
static bool ifr = false;
static long long start_time; 
static long long pre_time;
static atomic<uint32_t> cnt;
static atomic<uint64_t> get_conflict_cnt;
static atomic<uint32_t> get_cnt;
static atomic<uint64_t> _cnt_1;
static atomic<uint64_t> _cnt_2;
static atomic<uint64_t> _cnt_3;
static atomic<uint64_t> _cnt_4;
static atomic<uint64_t> _cnt_5;
static atomic<uint64_t> _cnt_6;
#endif

NvmEngine::NvmEngine(const std::string& path, FILE *_log_file) {
        init_thread_id = 0;
    #ifdef TEST
    _cnt_1 = 0;
    _cnt_2 = 0;
    _cnt_3 = 0;
    _cnt_4 = 0;
    _cnt_5 = 0;
    _cnt_6 = 0;
    start_time = getCurrentTime();
    pre_time = start_time;
    cnt = 0;
    get_conflict_cnt = 0;
    get_cnt = 0;
    #endif
    log_file = _log_file;
    struct stat tmp_var;
    if (stat(path.c_str(), &tmp_var) == 0) {
        fprintf(log_file,"recover init\n");
        fflush(log_file);
        //file exit             -- recorver db
        if ((key_addr = (char*) pmem_map_file(path.c_str(), 
            TOTAL_FILE_SIZE, PMEM_FILE_CREATE, 0666, nullptr, nullptr)) == NULL) {
            fprintf(log_file,"create or open key file fault\n");
            fflush(log_file);
            //exit(1);
        }
        char zero[KEY_ENTRY_SIZE];
        memset(zero, 0, KEY_ENTRY_SIZE);
	    for (idx = 0; idx < FILE_NUM; ++ idx) {
            file_end_offset[idx] = idx * SMALL_SLICE_NUM + 1;
        }
        memset(offset_hash, 0, sizeof(uint32_t) * HASH_TABLE_LENGTH);
        memset(in_list, 0, sizeof(bool) * HASH_TABLE_LENGTH);
        // recover file_end_offset and offset_hash
        uint64_t s_cnt = 0;
        for (idx = 0; idx < FILE_NUM; ++ idx) {
            while (file_end_offset[idx] < (idx + 1) * SMALL_SLICE_NUM) {
                memcpy(rbuffer, key_addr + file_end_offset[idx] * SLICE_SIZE, KEY_ENTRY_SIZE);
                if (memcmp(rbuffer, zero, KEY_ENTRY_SIZE) == 0) {
                    break;
                }
                uint8_t block_num = TO_UINT8(rbuffer);
                if (block_num == 0) {
                    break;
                }
                uint32_t cur_flag = TO_UINT32(rbuffer + 1);
                val_size = TO_UINT16(rbuffer + 5);
                char fbuffer[FLAG_SZ];
                memcpy(fbuffer, key_addr + file_end_offset[idx] * SLICE_SIZE + KEY_ENTRY_SIZE + val_size, FLAG_SZ);
                uint32_t tail_flag = TO_UINT32(fbuffer);
                if (cur_flag == 0 || cur_flag != tail_flag) {
                    ++ s_cnt;
                    stacks[s_cnt % THREAD_NUM][block_num - INDEX_OFFSET].push(file_end_offset[idx]);
                    file_end_offset[idx] += block_num;
                    continue;
                }
                // recover key_hash_table
                hash_result = TO_UINT32(rbuffer + 19) % HASH_TABLE_LENGTH;
                offset = offset_hash[hash_result];
                bool update = false;
                while (offset != 0) {
                    final_old_offset = offset;
                    memcpy(vbuffer, key_addr + final_old_offset * SLICE_SIZE, KEY_ENTRY_SIZE);
                    if (memcmp(rbuffer + 7, vbuffer + 7, WKEY_SIZE) == 0) {
                        ++ s_cnt;
                        uint32_t tmp_flag = TO_UINT32(vbuffer + 1);
                        if (tmp_flag > cur_flag) {
                            stacks[s_cnt % THREAD_NUM][block_num - INDEX_OFFSET].push(file_end_offset[idx]);
                        }
                        else {
                            stacks[s_cnt % THREAD_NUM][TO_UINT8(vbuffer) - INDEX_OFFSET].push(final_old_offset);
                            offset_hash[hash_result] = file_end_offset[idx];
                            update = true;
                        }
                        break;
                    }
                    if (++ hash_result == HASH_TABLE_LENGTH)    hash_result = 0;
                    offset = offset_hash[hash_result];
                }
                if (!update)
                    offset_hash[hash_result] = file_end_offset[idx];
                file_end_offset[idx] += block_num;
            }
        }
        //init hash table finish
    }
    else {
        memset(offset_hash, 0, sizeof(uint32_t) * HASH_TABLE_LENGTH);
        if ((key_addr = (char*) pmem_map_file(path.c_str(), 
            TOTAL_FILE_SIZE, PMEM_FILE_CREATE, 0666, nullptr, nullptr)) == NULL) {
            fprintf(log_file,"create or open key file fault\n");
            fflush(log_file);
        }
        for (idx = 0; idx < FILE_NUM; ++ idx){
            file_end_offset[idx] = idx * SMALL_SLICE_NUM + 1;
        }
        memset(in_list, 0, sizeof(bool) * HASH_TABLE_LENGTH);
    }
}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr, FILE *log_file) {
    NvmEngine* db = new NvmEngine(name, log_file);
    *dbptr = db;
    return Ok;
}

Status NvmEngine::Get(const Slice& key, std::string* value) {
    #ifdef TEST
    uint32_t ct = ++get_cnt;
    if (ct % 1000000 == 0){
        fprintf(log_file, "get  %u * M time total visist aep %lu times\n", get_cnt.load()/1000000, get_conflict_cnt.load());
        fflush(log_file);
    }
    if (get_cnt > 288000000){
        fprintf(log_file, "get  %u * M time total cost %lf s\n", get_cnt.load()/1000000, (getCurrentTime() - start_time)/1000.0);
        exit(1);
    }
    #endif
    if (cache.get(key,value) == Ok) {
        return Ok;
    }
    #ifdef TEST
    ++get_conflict_cnt;
    #endif
    hash_result = TO_UINT32(key.data() + 12) % HASH_TABLE_LENGTH;
    offset = offset_hash[hash_result];
    while (offset != 0) {
        if (in_list[hash_result]) {
            if (memcmp(kl.get(offset)._key, key.data(), WKEY_SIZE) == 0) {
                final_old_offset = kl.get(offset)._val_offset;
                val_size = kl.get(offset)._size;
                memcpy(vbuffer, key_addr + final_old_offset * SLICE_SIZE + KEY_ENTRY_SIZE, val_size);
                *value = string(vbuffer, val_size);
                cache.set(key, vbuffer, val_size);
                return Ok;
            }
        }
        else {
            final_old_offset = offset;
            memcpy(vbuffer, key_addr + final_old_offset * SLICE_SIZE, 256);
            if (memcmp(vbuffer + 7, key.data(), WKEY_SIZE) == 0) {
                val_size = TO_UINT16(vbuffer + 5);
                if (val_size > 256 - KEY_ENTRY_SIZE) {
                    memcpy(vbuffer + 256, key_addr + final_old_offset * SLICE_SIZE + 256, val_size - 256 + KEY_ENTRY_SIZE);
                } 
                *value = string(vbuffer + KEY_ENTRY_SIZE, val_size);
                cache.set(key, value->c_str(), val_size);
                return Ok;
            } 
        }
        if (++ hash_result == HASH_TABLE_LENGTH)    hash_result = 0;
        offset = offset_hash[hash_result];
    }
    return NotFound;
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {
    #ifdef TEST
    uint64_t ct = ++cnt;
    if (ct % 1000000 == 0) {
        fprintf(log_file, "set the %u M data cost total %.2lf s\n", cnt/1000000, (getCurrentTime() - start_time) / 1000.0);
        fprintf(log_file, "set one data cost %.2lf s\n",(getCurrentTime() - pre_time)/ 1000.0);
        fprintf(log_file, "visit in list %lf M times in aep %lf M times\n",
                _cnt_1/1000000.0, _cnt_2/1000000.0);
        process_mem_usage();
        pre_time = getCurrentTime();
        if (ifr){
            fprintf(log_file, "wr new value size bigger | lesser than num is %lu M| % lu M\n",
                    _cnt_3/1000000, _cnt_4/1000000);
            fprintf(log_file, "wr old value size bigger | lesser than num is %lu M| % lu M\n",
                    _cnt_5/1000000, _cnt_6/1000000);
        }
        if (cnt/1000000 == 402)
            ifr = true;
        fflush(log_file);
    }
    if (ifr) {
        if (value.size() < 128)
            _cnt_3 ++;
        else
            _cnt_4 ++;
    }
    #endif
    if (!init_flag) {
        int end = init_thread_id ++;
        thread_id = end % THREAD_NUM;
        init_flag = true;
    }

    cache.update(key, value);
    idx = TO_UINT32(key.data() + 12) % FILE_NUM;
    bool update_key = false;

    uint8_t block_num = get_block_num(value.size());
    uint8_t old_block_num;
    uint8_t space_block_num;
    uint32_t new_cnt = 1;
    val_size = value.size();
    final_new_offset = get_block_offset(block_num, space_block_num);
    key_hash_result = TO_UINT32(key.data() + 12) % KEY_CACHE_TABLE_LENGTH;
    
    hash_result = TO_UINT32(key.data() + 12) % HASH_TABLE_LENGTH;
    offset = offset_hash[hash_result];
    while (offset != 0) {
        if (in_list[hash_result]) {
            #ifdef TEST
            _cnt_1 ++;
            #endif
            if (memcmp(kl.get(offset)._key, key.data(), WKEY_SIZE) == 0) {
                #ifdef TEST
                if(ifr){
                    if (kl.get(offset)._size < 128)
                        _cnt_5 ++;
                    else
                        _cnt_6 ++;
                }
                #endif
                old_block_num = kl.get(offset)._block_num;
                final_old_offset = kl.get(offset)._val_offset;
                update_key = true;
                kl.update(offset, value.size(), block_num, final_new_offset);
                new_cnt = kl.get(offset)._cnt;
                break;
            }
        }
        else {
            #ifdef TEST
            _cnt_2 ++;
            #endif
            final_old_offset = offset;
            memcpy(rbuffer, key_addr + final_old_offset * SLICE_SIZE, KEY_ENTRY_SIZE);
            if (memcmp(rbuffer + 7, key.data(), WKEY_SIZE) == 0) {
                //update key
                #ifdef TEST
                if (ifr) {
                    uint16_t size = TO_UINT16(rbuffer + 5);
                    if (size < 128)
                        _cnt_5 ++;
                    else
                        _cnt_6 ++;
                }
                #endif
                old_block_num = TO_UINT8(rbuffer);
                new_cnt = TO_UINT32(rbuffer + 1) + 1;
                update_key = true;
                offset_hash[hash_result] = final_new_offset;
                break;
            }
        }
        if (++ hash_result == HASH_TABLE_LENGTH)    hash_result = 0;
        offset = offset_hash[hash_result];
    }
    if (!update_key) {
        uint32_t pos = kl.add(thread_id, key.data(), value.size(), block_num, final_new_offset);
        if (pos != 0) {
            offset_hash[hash_result] = pos;
            in_list[hash_result] = true;
        }
        else {
            offset_hash[hash_result] = final_new_offset;
            in_list[hash_result] = false;
        }
    }
    // find a  offset pos *****it's to add  a new kv now******

    memcpy(key_addr + final_new_offset * SLICE_SIZE, &space_block_num, 1);
    memcpy(key_addr + final_new_offset * SLICE_SIZE + 1, &new_cnt, FLAG_SZ);
    memcpy(key_addr + final_new_offset * SLICE_SIZE + 5, &val_size, 2);
    memcpy(key_addr + final_new_offset * SLICE_SIZE + 7, key.data(), WKEY_SIZE);
    memcpy(key_addr + final_new_offset * SLICE_SIZE + KEY_ENTRY_SIZE, value.data(), val_size);
    memcpy(key_addr + final_new_offset * SLICE_SIZE + KEY_ENTRY_SIZE + val_size, &new_cnt, FLAG_SZ);
    pmem_persist(key_addr + final_new_offset * SLICE_SIZE, KEY_ENTRY_SIZE + val_size + FLAG_SZ);

    if (update_key) {
        stacks[thread_id][old_block_num - INDEX_OFFSET].push(final_old_offset);
    }

    return Ok;
}

uint32_t NvmEngine::get_block_offset(int block_num, uint8_t& space_block_num){
    for (int i = block_num - INDEX_OFFSET; i < QUEUE_NUM; ++ i) {
        if (!stacks[thread_id][i].empty()) {
            uint32_t r = stacks[thread_id][i].top();
            stacks[thread_id][i].pop();
            space_block_num = i + INDEX_OFFSET;
            return r;
        }
    }
    if (file_end_offset[idx] + block_num < (idx + 1) * SMALL_SLICE_NUM) {
        lock_guard<spin_mutex> lock(lock_mutex[idx]);
        uint32_t pos = file_end_offset[idx];
        file_end_offset[idx] += block_num;
        space_block_num = block_num;
        return pos;
    }
    fprintf(log_file,"no more space\n");
    fflush(log_file);
    exit(1);
}
int NvmEngine::get_block_num(uint32_t val_size) {
    uint32_t size = val_size + KEY_ENTRY_SIZE + FLAG_SZ;
    if (size % SLICE_SIZE == 0)
        return size / SLICE_SIZE;
    else
        return 1 + size / SLICE_SIZE;
}
#ifdef TEST
void NvmEngine::process_mem_usage()
{
   using std::ios_base;
   using std::ifstream;
   using std::string;

   double vm_usage     = 0.0;
   double resident_set = 0.0;

   // 'file' stat seems to give the most reliable results
   //
   ifstream stat_stream("/proc/self/stat",ios_base::in);

   // dummy vars for leading entries in stat that we don't care about
   //
   string pid, comm, state, ppid, pgrp, session, tty_nr;
   string tpgid, flags, minflt, cminflt, majflt, cmajflt;
   string utime, stime, cutime, cstime, priority, nice;
   string O, itrealvalue, starttime;

   // the two fields we want
   //
   unsigned long vsize;
   long rss;

   stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
               >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
               >> utime >> stime >> cutime >> cstime >> priority >> nice
               >> O >> itrealvalue >> starttime >> vsize >> rss; // don't care about the rest

   stat_stream.close();

   long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024; // in case x86-64 is configured to use 2MB pages
   vm_usage     = vsize / 1024.0;
   resident_set = rss * page_size_kb;

   fprintf(log_file, "VM: %lf RSS: %lf\n", vm_usage, resident_set);
   fflush(log_file);
}
#endif

NvmEngine::~NvmEngine() {}
