package forestdb

//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//#include <libforestdb/forestdb.h>
import "C"

import (
	"time"
	"unsafe"
)

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

type CompactOpt uint8

const (
	COMPACT_MANUAL CompactOpt = 0
	COMPACT_AUTO   CompactOpt = 1
)

// Config wraps ForestDB's configuration options
type Config struct {
	// Chunk size (bytes) that is used to build B+-tree at each level.
	// It is set to 8 bytes by default and has a min value of 4 bytes
	// and a max value of 64 bytes.
	// This is a local config to each ForestDB database instance.
	ChunkSize uint16

	// Size of block that is a unit of IO operations.
	// It is set to 4KB by default and has a min value of 1KB and a max value of
	// 128KB. This is a global config that is used across all ForestDB database
	// instances.
	BlockSize uint32

	// Buffer cache size in bytes. If the size is set to zero, then the buffer
	// cache is disabled. This is a global config that is used across all
	// ForestDB database instances. 128MB by default.
	BufferCacheSize uint64

	// WAL index size threshold in memory (4096 entries by default).
	// This is a local config to each ForestDB database instance.
	WALThreshold uint64

	// Flag to enable flushing the WAL whenever it reaches its threshold size.
	// This reduces memory usage when a lot of data is written before a commit.
	// Disabled by default.
	WALFlushBeforeCommit bool

	// Interval for purging logically deleted documents.
	// It is set to 0 (purge during next compaction) by default.
	// Set values are always floored to the next second interval.
	// This is a local config to each ForestDB database instance.
	PurgingInterval time.Duration

	// Set the sequence B+-Tree mode. Defaults to SEQTREE_USE.
	// This is a local config to each ForestDB database instance.
	SeqTree SeqTreeOpt

	// Flag to enable synchronous or asynchronous commit options.
	// This is a local config to each ForestDB database instance.
	// Uses a synchronous commit by default.
	Durability DurabilityOpt

	// Flags for opening the DB. It can be used for specifying read-only mode.
	// This is a local config to each ForestDB database instance.
	// By default, set to OPEN_FLAG_CREATE
	OpenFlags OpenFlags

	// Maximum size (bytes) of temporary buffer for compaction (4MB by default).
	// This is a local config to each ForestDB database instance.
	CompactionBufferMaxSize uint32

	// Skip cleaning all the cached blocks in the global buffer cache when a
	// database file is closed. This is a global config that is used across
	// all ForestDB database instances.
	SkipCacheCleanupOnClose bool

	// Compress the body of document when it is written on disk using snappy.
	// The compression is disabled by default. This is a global config that is
	// used across all ForestDB database instances.
	CompressDocumentBody bool

	// Sets compaction mode for the file. Defaults to: COMPACT_MANUAL.
	// This is a local config to each ForestDB database instance.
	CompactionMode CompactOpt

	// Compaction threshold in the unit of percentage (%). It can be calculated
	// as '(stale data size)/(total file size)'. The compaction daemon triggers
	// compaction if this threshold is satisfied. Defaults to 30%.
	// Compaction will not be performed when this value is set to zero or 100.
	// This is a local config to each ForestDB database instance.
	CompactionThreshold uint8

	// The minimum filesize to perform compaction. Defaults to 1M.
	// This is a local config to each ForestDB database instance.
	CompactionMinFileSize uint64

	// Duration that the compaction daemon periodically wakes up. Defaults to 15s.
	// Set values are always floored to the next second interval.
	// This is a global config that is used across all ForestDB database instances.
	CompactionSleep time.Duration

	CustomCompareFixed    unsafe.Pointer
	CustomCompareVariable unsafe.Pointer
}

// DefaultConfig creates the default configuration
func DefaultConfig() *Config {
	return &Config{
		ChunkSize:       8,
		BlockSize:       4 * 1024,
		BufferCacheSize: 128 * 1024 * 1024,
		WALThreshold:    4 * 1024,
		Durability:      DRB_NONE,
		OpenFlags:       OPEN_FLAG_CREATE,
		SeqTree:         SEQTREE_USE,

		CompactionMode:          COMPACT_MANUAL,
		CompactionBufferMaxSize: 4 * 1024 * 1024,
		CompactionThreshold:     30,
		CompactionMinFileSize:   1024 * 1024,
		CompactionSleep:         15 * time.Second,
	}
}

func (c *Config) native() *C.fdb_config {
	if c == nil {
		c = DefaultConfig()
	}

	n := C.fdb_get_default_config()
	n.chunksize = C.uint16_t(c.ChunkSize)
	n.blocksize = C.uint32_t(c.BlockSize)
	n.buffercache_size = C.uint64_t(c.BufferCacheSize)
	n.wal_threshold = C.uint64_t(c.WALThreshold)
	n.wal_flush_before_commit = C.bool(c.WALFlushBeforeCommit)
	n.purging_interval = C.uint32_t(c.PurgingInterval / time.Second)
	n.seqtree_opt = C.fdb_seqtree_opt_t(c.SeqTree)
	n.durability_opt = C.fdb_durability_opt_t(c.Durability)
	n.flags = C.fdb_open_flags(c.OpenFlags)
	n.compaction_buf_maxsize = C.uint32_t(c.CompactionBufferMaxSize)
	n.cleanup_cache_onclose = C.bool(!c.SkipCacheCleanupOnClose)
	n.compress_document_body = C.bool(c.CompressDocumentBody)
	n.compaction_mode = C.fdb_compaction_mode_t(c.CompactionMode)
	n.compaction_threshold = C.uint8_t(c.CompactionThreshold)
	n.compaction_minimum_filesize = C.uint64_t(c.CompactionMinFileSize)
	n.compactor_sleep_duration = C.uint64_t(c.CompactionSleep / time.Second)
	if c.CustomCompareFixed != nil {
		n.cmp_fixed = C.fdb_custom_cmp_fixed(c.CustomCompareFixed)
	}
	if c.CustomCompareVariable != nil {
		n.cmp_variable = C.fdb_custom_cmp_variable(c.CustomCompareVariable)
	}

	return &n
}
