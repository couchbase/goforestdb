package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#include <forestdb/forestdb.h>
import "C"

type OpenFlags uint32

const (
	OPEN_FLAG_CREATE OpenFlags = 1
	OPEN_FLAG_RDONLY OpenFlags = 2
)

type SeqTreeOpt uint8

const (
	SEQTREE_NOT_USE SeqTreeOpt = 0
	SEQTREE_USE     SeqTreeOpt = 1
)

type DurabilityOpt uint8

const (
	DRB_NONE          DurabilityOpt = 0
	DRB_ODIRECT       DurabilityOpt = 0x1
	DRB_ASYNC         DurabilityOpt = 0x2
	DRB_ODIRECT_ASYNC DurabilityOpt = 0x3
)

// ForestDB config options
type Config struct {
	config *C.fdb_config
}

func (c *Config) ChunkSize() uint16 {
	return uint16(c.config.chunksize)
}

func (c *Config) SetChunkSize(s uint16) {
	c.config.chunksize = C.uint16_t(s)
}

func (c *Config) BlockSize() uint32 {
	return uint32(c.config.blocksize)
}

func (c *Config) SetBlockSize(s uint32) {
	c.config.blocksize = C.uint32_t(s)
}

func (c *Config) BufferCacheSize() uint64 {
	return uint64(c.config.buffercache_size)
}

func (c *Config) SetBufferCacheSize(s uint64) {
	c.config.buffercache_size = C.uint64_t(s)
}

func (c *Config) WalThreshold() uint64 {
	return uint64(c.config.wal_threshold)
}

func (c *Config) SetWalThreshold(s uint64) {
	c.config.wal_threshold = C.uint64_t(s)
}

func (c *Config) WalFlushBeforeCommit() bool {
	return bool(c.config.wal_flush_before_commit)
}

func (c *Config) SetWalFlushBeforeCommit(b bool) {
	c.config.wal_flush_before_commit = C.bool(b)
}

func (c *Config) PurgingInterval() uint32 {
	return uint32(c.config.purging_interval)
}

func (c *Config) SetPurgingInterval(s uint32) {
	c.config.purging_interval = C.uint32_t(s)
}

func (c *Config) SeqTreeOpt() SeqTreeOpt {
	return SeqTreeOpt(c.config.seqtree_opt)
}

func (c *Config) SetSeqTreeOpt(o SeqTreeOpt) {
	c.config.seqtree_opt = C.fdb_seqtree_opt_t(o)
}

func (c *Config) DurabilityOpt() DurabilityOpt {
	return DurabilityOpt(c.config.durability_opt)
}

func (c *Config) SetDurabilityOpt(o DurabilityOpt) {
	c.config.durability_opt = C.fdb_durability_opt_t(o)
}

func (c *Config) OpenFlags() OpenFlags {
	return OpenFlags(c.config.flags)
}

func (c *Config) SetOpenFlags(o OpenFlags) {
	c.config.flags = C.fdb_open_flags(o)
}

func (c *Config) CompactionBufferSizeMax() uint32 {
	return uint32(c.config.compaction_buf_maxsize)
}

func (c *Config) SetCompactionBufferSizeMax(s uint32) {
	c.config.compaction_buf_maxsize = C.uint32_t(s)
}

func (c *Config) CleanupCacheOnClose() bool {
	return bool(c.config.cleanup_cache_onclose)
}

func (c *Config) SetCleanupCacheOnClose(b bool) {
	c.config.cleanup_cache_onclose = C.bool(b)
}

func (c *Config) CompressDocumentBody() bool {
	return bool(c.config.compress_document_body)
}

func (c *Config) SetCompressDocumentBody(b bool) {
	c.config.compress_document_body = C.bool(b)
}

func (c *Config) CompactionThreshold() uint8 {
	return uint8(c.config.compaction_threshold)
}

func (c *Config) SetCompactionThreshold(s uint8) {
	c.config.compaction_threshold = C.uint8_t(s)
}

func (c *Config) CompactionMinimumFilesize() uint64 {
	return uint64(c.config.compaction_minimum_filesize)
}

func (c *Config) SetCompactionMinimumFilesize(s uint64) {
	c.config.compaction_minimum_filesize = C.uint64_t(s)
}

func (c *Config) CompactorSleepDuration() uint64 {
	return uint64(c.config.compactor_sleep_duration)
}

func (c *Config) SetCompactorSleepDuration(s uint64) {
	c.config.compactor_sleep_duration = C.uint64_t(s)
}

// DefaultConfig gets the default ForestDB config
func DefaultConfig() *Config {
	config := C.fdb_get_default_config()
	return &Config{
		config: &config,
	}
}
