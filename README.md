# Tair Rank13 Solution

阿里云-天池：第二届数据库大赛 - Tair性能挑战赛 

第13名队伍：以上队伍成绩作废 

[比赛首页](<https://tianchi.aliyun.com/competition/entrance/531820/introduction>)

**初赛成绩**：73.480s

**初赛排名**：16 / 2170

**复赛成绩**：59.011s 

**复赛排名**：13 / 2170

## 1. 赛题描述与分析

本题设计一个基于Intel傲腾持久化内存 (AEP) 的Key-Value单机引擎，支持 `Set` 和 `Get` 数据接口，同时对于热点访问具有良好的性能。

### 1.1 初赛

评测程序调用选手实现的接口，启动16个线程进行写入和读取数据，对所用时间进行排名。评测包括两个阶段：正确性评测和性能评测。

初赛使用存储空间为：4G DRAM 和 74G AEP

每个线程分别写入约 48M 个Key大小为 16Bytes，Value大小为 80Bytes 的KV对象，接着以 95:5 的读写比例访问调用 48M 次。其中 95% 的读访问具有热点的特征，大部分读访问集中在少量Key上面。

> **分析** ：
>
> KV数据总数量为：16 * 48M = 16 * 48 * 2^20 = 0.75 * 2^30 个 = 0.75G 个
>
> - 所以表示KV在AEP中位置的offset至少为 4Bytes (32 bits)，可分128片，每片中offset可用 24bits 表示
> - 所有offset总大小为：0.75 * 2^30 * 4 = 3 GB （分片后 0.75 * 2^30 * 3 = 2.25 GB）
> - DRAM总共只有4G，且要考虑到hash table碰撞因子，可见DRAM存储空间很紧张
>
> Key总大小为：16 * 48 * 2^20 * 16 = 12 GB
>
> Value总大小为： 16 * 48 * 2^20 * 80 = 60 GB
>
> KV总大小为：12 GB + 60 GB = 72 GB
>
> - AEP总共只有74GB，可见AEP存储空间非常紧张
>
> 95% 的读访问具有热点特性
>
> - 需要设计合理的cache，但是DRAM空间紧张，要处理好空间与效率上的trade-off

### 1.2 复赛

复赛使用存储空间为：8G DRAM 和 64G AEP

复赛要求数据具有持久化和可恢复能力，确保在接口调用后，即使掉电也能保证数据的完整性和正确恢复。包括正确性评测、持久化评测和性能评测。

开启 16 个线程并发调用 24M 次Set操作，key大小为 16Bytes，Value大小范围为 80-1024Bytes，分布为：

- 约55% 的Value长度为 80-128 Bytes
- 约 25% 的Value长度为 129-256 Bytes
- 约 15% 的Value长度为 257-512 Bytes
- 约 5% 的Value长度为 513-1024 Bytes

总体数据写入量大约在 75G 左右

接着会进行10次读写混合测试，取最慢一次的结果作为成绩，每次都会开启16个线程以 75% : 25% 的读写比例调用 24M 次。其中 75% 的读访问具有热点的特征，大部分的读访问集中在少量的key上面。

**pmem 特性**：

- DRAM 和 AEP间的cache line 大小为 64 bytes
- 实际物理访问量是 256 bytes
- 只能保证 8bytes 的原子性写入

> **分析**：
>
> KV数据总数量为：16 * 24M = 16 * 24 * 2^20 = 0.375 * 2^30 个 = 0.375G 个
>
> - 表示KV在AEP中位置的offset使用 4Bytes (32 bits)
> - 所有offset总大小为：0.375 * 2^30 * 4 = 1.5 GB
> - 可见复赛DRAM空间是比较富余的 
>
> 总体数据写入量约为 75G 左右，而AEP大小为 64G
>
> - 要做好Value更新后的空间回收
>
> 复赛有了持久化评测
>
> - 重新启动时需要根据AEP中数据重建DRAM中Hash Index
>
> PMEM只能保证 8Bytes 的原子性写入，而KV都大于8Bytes，写入过程中可能发生断电
>
> - 要对写入进行原子性保护
>
> `pmem_persist = pmem_flush + pmem_drain` ，但是drain会阻塞一些操作
>
> `pmem_flush` 会刷新CPU write buffer，`pmem_drain` 会等待数据写入介质
>
> - 更好的方法对不同的变量 `pmem_flush`，最后一并调用 `pmem_drain` ，以将阻塞带来的问题降到最低 

## 2. 初赛设计概述

### 2.1. 文件分片

将文件分为128片，各片内分别计算offset。128=2^7，可以使得表示offset的bit数从30降为23。有效节省了 DRAM 的空间使用（3GB -> 2.25GB）。

### 2.2. Hash 计算

因为数据key分布均匀，所以可以直接取key中的几位作为hash function。

先用key中7个bit将其分片，再用另外23个bit计算片内offset。

### 2.3. 冲突解决

使用线型探测法。若使用链地址法，AEP空间不足以为每个KV存储指针。

### 2.4. 优化

#### 2.4.1. 降低装载因子

装载因子越低，Hash计算后的冲突越少。我们使用了75%的装载因子。

#### 2.4.2. cache

我们尝试使用LRU，但是LRU实现往往包含Map和双向链表，需要占用大量空间。LRU里可以存储的KV并不多，带来的优化效果还不如降低装载因子。

最后设计了一个简单的cache，通过hash table对kv进行存储，get时hash冲突时直接修改KV，set时若该key在cache中，则修改，否则不修改。取得了大约10s的提升。

#### 2.4.3. 组合写入

将要写入文件的KV先在DRAM中组合成 256B 的倍数（pmem单词写入大小为256B），再一起写入文件，可以获得大约20s的提升。

#### 2.4.4. 文件分片

文件分片不仅帮助我们节省了DRAM空间，还降低hash table的锁争用，大大提升了性能。

## 3. 复赛设计概述

### 3.1. Hash 

hash function为取key的32个bit，然后模hash table的数组长度。

hash table采用线性探测法解决冲突。

### 3.2. Cache

建立两级cache，经实验比LRU性能更好。

#### 3.2.1. 第一级cache

第一级cache存储完整的key和value，数量大约为67w。

get时会添加KV到第一级cache中，key hash冲突时也更新。

set时若该key已存在于第一级cache，则更新值；否则不将其加入cache。此举是为了不破坏读热点。

#### 3.2.2. 第二级cache

第二级cache是一个KeyList，存储key及其value对应的offset、value_size和block_num。KeyList大大减少了hash冲突。

总共约1亿个，分线程分配从而避免线程争用。DB会有一个in_list bool数组来镖师keyList的slot是否被用。

这一级cache也可以算做索引的一部分。

添加不存在key时，会优先往keyList里添加，keyList满了才会存入hash table。

更新key时，若key在keyList中，也只是更新keyList。

get时，若in_list数组标识key对应hash值已在keyList中，若key匹配则直接从keyList中取出offset等数据，从AEP中读到value；否则访问hash table。这样就减少了hash table中的冲突次数。

### 3.3. AEP中KV存储

cnt_flag (4B) | block_num (1B) | val_size (2B) | key (16B) | value | cnt_flag (4B)

cnt_flag用于恢复时判断KV存储完整性，每次更新key时，对应的cnt_flag会递增1。故障恢复时，若头部cnt_flag != 尾部cnt_flag，则此KV是损坏的，不将其恢复。

这种方法相比于checksum省去了计算开销，相比于先persist KV再persist flag省去了一次pmem_persist操作。缺点是更新次数太多的话4Bytes会不够递增（这个问题在比赛中不会暴露，因为数据量已知）。

block_num为整个KV entry占用的单位回收块的个数。

### 3.4. GC

一个回收块大小为32B。

每个线程都维护几个threadLocal的stack，不同的栈里存储不同个数回收块组成的大回收块。除了threadLocal的回收空间，还包括一个Global Space，若一个线程中回收空间不够了就可以取Global Space中的空间。因为Global Space的访问需要加锁，所以优先使用threadLocal的回收空间。

这种ThreadLocal + Global Space的回收机制有效减少了线程对锁的争用，给性能带来了巨大提升。

更新Key时，将其就空间回收，再根据其新value的长度分配满足长度的最小回收块。

### 3.5. 故障恢复

故障恢复过程即为恢复hash table的过程，每读到一条entry：

- 通过其首尾cnt_flag判断entry写入是否完整
  - 若不完整，回收entry所占用的空间
  - 若完整，在hash table中查询该key
    - 若key已存在，比较两者的cnt_flag，值小者为旧数据，将其空间回收，新值的相关数据写入hash table
    - 若key不存在，新值的相关数据写入hash table

读到entry各位为0时，恢复完成。

### 3.6. Get 逻辑

1. 查找cache，若key存在，返回value
2. 计算key的hash_result值和其在hash table中的offset
3. 若offset == 0，返回NotFound
4. 通过in_list[hash_result]判断key是否在keyList中：
   1. 若在keyList，从KeyList读取value在AEP中位置及其他信息，从AEP读出value并返回
   2. 否则从AEP读取key，判断是否匹配，若匹配，读出value并返回。
   3. 找到key的两种情况都要更新cache
   4. 否则递增hash_result进行线性探测法，到offset==0时推出循环，返回NotFound

### 3.7. Set 逻辑

1. 先更新cache（仅在key已在cache的情况下更新）
2. 通过value_size计算block_num，并取得一个大小合适的存储块
3. 计算key的hash_result值和其在hash table中的offset
4. 先判断key是否在keyList
   1. 若在KeyList，用新value及其相关数据、新的存储位置更新keyList
   2. 若不在，则从AEP中读出key进行匹配，若匹配，读取器原来cnt_flag并加1，更新hash table
   3. 若AEP中没查到，递增hash_result进行线性探测法。
5. 退出循环时若没有找到key，则优先将其添加到KeyList中，若KeyList已满，则添加到hash table中
6. 将新的entry通过pmem_persist写入AEP
7. 若key存在，将旧空间回收（必须确保新值写成功才可以回收旧空间）

---

## 4. 可改进之处

- **分片顺序**写入能有效提高不规整（非 256B） 写入的带宽。
- **写入地址、写入量对齐** 能有效提高带宽。
- 将pmem划分为64个片区，并将写入地址和写入量都对齐到64B可提升效率
- 空间分配策略应优先从pmem末尾分配，末尾没空间了再从回收池中分配

- **TLB预热**：在构造函数中将文件顺序读一遍，可以起到TLB预热的作用，可提升5s左右

