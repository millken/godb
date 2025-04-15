package godb

import (
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	art "github.com/millken/godb/internal/radixtree"
	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
)

const (
	fileModePerm = 0644
	dbFileSuffix = ".db"
	maxKeySize   = math.MaxUint8
	maxValueSize = 64 * MB
)

var (
	ErrEmptyKey = errors.New("empty key")

	ErrInvalidKey     = errors.New("invalid key")
	ErrInvalidTTL     = errors.New("invalid ttl")
	ErrExpiredKey     = errors.New("key has expired")
	ErrDatabaseClosed = errors.New("database closed")
)

type DB struct {
	path     string
	opts     *option
	segments []*segment
	current  *segment
	idx      *art.Tree[index]
	mu       sync.RWMutex
	closed   bool

	compactionInterval time.Duration
	compacting         atomic.Bool
	compactCh          chan struct{}
	stopCh             chan struct{}
	wg                 sync.WaitGroup
}

// Open opens a database at the given path.
func Open(dbpath string, options ...Option) (*DB, error) {
	if err := os.MkdirAll(dbpath, 0777); err != nil {
		return nil, err
	}

	opts := defaultOption()
	for _, opt := range options {
		opt(opts)
	}
	db := &DB{
		path:               dbpath,
		opts:               opts,
		idx:                art.New[index](),
		compactionInterval: 1 * time.Hour, // 默认1小时压缩一次
		compactCh:          make(chan struct{}),
		stopCh:             make(chan struct{}),
	}
	entries, err := os.ReadDir(dbpath)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == dbFileSuffix {
			seg := newSegment(0, path.Join(dbpath, entry.Name()), db.idx)
			if err := seg.Open(); err != nil {
				return nil, err
			}
			db.segments = append(db.segments, seg)
			slices.SortFunc(db.segments, func(a, b *segment) int {
				return int(a.ID() - b.ID())
			})
		}
	}
	if len(db.segments) == 0 {
		filename := fmt.Sprintf("%04x%s", 0, dbFileSuffix)
		// Generate new empty segment.
		segment, err := createSegment(0, filepath.Join(db.path, filename), db.idx)
		if err != nil {
			return nil, err
		}
		db.segments = append(db.segments, segment)
	}
	db.current = db.segments[len(db.segments)-1]
	db.wg.Add(1)
	go db.compactionLoop()
	return db, nil
}

// activeSegment returns the last segment.
func (db *DB) activeSegment() *segment {
	if len(db.segments) == 0 {
		return nil
	}
	return db.segments[len(db.segments)-1]
}

// Put put the value of the key to the db
func (db *DB) Put(key, value []byte) error {
	if len(value) > int(maxValueSize) {
		return ErrValueTooLarge
	}
	if err := db.Delete(key); err != nil {
		return err
	}
	return db.set(key, value, putted)
}

// Get gets the value of the key from the db
func (db *DB) Get(key []byte) ([]byte, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	idx, found := db.idx.Get(key)
	if !found {
		return nil, ErrKeyNotFound
	}
	return db.get(idx)
}

// get gets the value of the index from the db
func (db *DB) get(idx index) ([]byte, error) {
	var segment *segment
	for _, seg := range db.segments {
		if seg.ID() == idx.segment() {
			segment = seg
			break
		}
	}
	return segment.ReadAt(idx.offset())
}

func (db *DB) createSegment() (*segment, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	// Generate a new sequential segment identifier.
	var id uint16
	if len(db.segments) > 0 {
		id = db.segments[len(db.segments)-1].ID() + 1
	}
	filename := fmt.Sprintf("%04x%s", id, dbFileSuffix)

	// Generate new empty segment.
	segment, err := createSegment(id, filepath.Join(db.path, filename), db.idx)
	if err != nil {
		return nil, err
	}
	db.segments = append(db.segments, segment)

	return segment, nil
}

func (db *DB) Delete(key []byte) error {
	if err := validateKey(key); err != nil {
		return err
	}
	idx, ok := db.idx.Get(key)
	if !ok {
		return nil
	}
	db.mu.RLock()
	var targetSegment *segment
	for _, seg := range db.segments {
		if seg.ID() == idx.segment() {
			targetSegment = seg
			break
		}
	}
	db.mu.RUnlock()
	if targetSegment == nil {
		return fmt.Errorf("segment %d not found for key %s", idx.segment(), key)
	}
	if err := targetSegment.updateRecordState(idx.offset(), deleted); err != nil {
		return err
	}

	db.idx.Delete(key)
	return nil
}

func (db *DB) set(key, value []byte, state state) error {
	var err error
	segment := db.activeSegment()
	if segment == nil || !segment.CanWrite() {
		if segment, err = db.createSegment(); err != nil {
			return err
		}
	}
	if err = segment.Write(key, value, state); err != nil {
		return err
	}

	if db.opts.fsync {
		if err := segment.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// Close releases all db resources.
func (db *DB) Close() error {
	var err error
	// close(db.stopCh)
	// db.wg.Wait()
	for _, s := range db.segments {
		if e := s.Close(); e != nil && err == nil {
			err = e
		}
	}
	return err
}
func (db *DB) writeBatch(recs records) error {
	offset := db.current.size
	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)
	for _, rec := range recs {
		if err := rec.marshalToBuffer(buffer); err != nil {
			return err
		}
		rec.offset = offset
		rec.seg = db.current.ID()
		offset += rec.size()
	}
	if err := db.current.write(buffer.Bytes()); err != nil {
		return err
	}

	return nil
}

func validateKey(key []byte) error {
	ks := len(key)
	if ks == 0 {
		return ErrEmptyKey
	} else if len(key) > maxKeySize {
		return ErrKeyTooLarge
	}
	return nil
}

func (db *DB) compactionLoop() {
	defer db.wg.Done()

	ticker := time.NewTicker(db.compactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := db.Compact(); err != nil {
				// 记录错误但不中断压缩循环
				log.Printf("Compaction error: %v", err)
			}
		case <-db.compactCh:
			// 手动触发压缩
			if err := db.Compact(); err != nil {
				log.Printf("Manual compaction error: %v", err)
			}
		case <-db.stopCh:
			return
		}
	}
}

// TriggerCompaction 手动触发压缩
func (db *DB) TriggerCompaction() {
	select {
	case db.compactCh <- struct{}{}:
	default:
		// 已经有压缩任务在排队
	}
}

// Compact 执行数据库压缩操作，优化锁定策略
func (db *DB) Compact() error {
	// 确保同一时间只有一个压缩任务
	if !db.compacting.CompareAndSwap(false, true) {
		return nil
	}
	defer db.compacting.Store(false)

	// // 第一阶段：获取当前段的快照，最小化锁定时间
	db.mu.RLock()
	segments := append([]*segment{}, db.segments[:len(db.segments)-1]...)
	// activeSegID := db.activeSegment().ID()
	db.mu.RUnlock()

	if len(segments) == 0 {
		return nil // 没有可压缩的段
	}
	// stats := compactionStats{
	// 	SegmentsTotal: len(segments),
	// }
	// segmentStats := make([]*segmentStat, 0, len(segments))
	// for _, seg := range segments {
	// 	if seg.ID() == activeSegID {
	// 		continue // 跳过当前活动段
	// 	}
	// 	segStat := &segmentStat{
	// 		segment: seg,
	// 	}
	// 	if err := seg.ScanHdr(func(h hdr) error {
	// 		return nil
	// 	}); err != nil {
	// 		return err
	// 	}
	// }
	// // 创建临时索引，收集所有键的最新值（不需要锁定DB）
	// tempIdx := map[string]struct{ segID, offset uint64 }{}

	// // 扫描所有段，从最新到最旧
	// for i := len(segmentsSnapshot) - 1; i >= 0; i-- {
	// 	seg := segmentsSnapshot[i]

	// 	err := seg.Scan(func(record *record) error {
	// 		key := string(record.key)
	// 		if record.typ == deleted || _, exists := tempIdx[key]; exists {
	// 			return nil
	// 		}

	// 		// 在索引中存储段ID和偏移量
	// 		tempIdx[key] = struct{ segID, offset uint64 }{
	// 			segID:  seg.id,
	// 			offset: record.offset,
	// 		}
	// 		return nil
	// 	})

	// 	if err != nil {
	// 		return err
	// 	}
	// }

	// // 创建新段（不需要锁定DB）
	// compactedSegmentID := uint64(time.Now().UnixNano())
	// filename := fmt.Sprintf("segment_%d.db", compactedSegmentID)
	// compactedSeg, err := createSegment(compactedSegmentID, filepath.Join(db.path, filename), nil)
	// if err != nil {
	// 	return err
	// }

	// // 创建新索引（不需要锁定DB）
	// newIdx := radixtree.New[*Index]()

	// // 写入所有有效数据到新段
	// validRecords := 0
	// for key, idx := range tempIdx {
	// 	// 根据段ID找到对应的段
	// 	var seg *segment
	// 	for _, s := range segmentsSnapshot {
	// 		if s.id == idx.segID {
	// 			seg = s
	// 			break
	// 		}
	// 	}

	// 	if seg == nil {
	// 		continue // 段已不存在，可能在第一阶段之后被修改
	// 	}

	// 	// 从原始段读取记录
	// 	record, err := seg.Read(idx.offset)
	// 	if err != nil {
	// 		continue // 如数据损坏则跳过
	// 	}

	// 	// 写入新段
	// 	offset, err := compactedSeg.Write(record)
	// 	if err != nil {
	// 		compactedSeg.Close()
	// 		os.Remove(compactedSeg.path)
	// 		return err
	// 	}

	// 	// 更新临时索引
	// 	newIdx.Put([]byte(key), newIndex(compactedSegmentID, offset))
	// 	validRecords++
	// }

	// // 第二阶段：短暂锁定替换段和索引
	// db.mu.Lock()
	// // 检查是否有新段被添加
	// if len(db.segments) > len(segmentsSnapshot) {
	// 	// 将新增段添加到压缩段中
	// 	for i := len(segmentsSnapshot); i < len(db.segments); i++ {
	// 		newSeg := db.segments[i]
	// 		newSeg.Scan(func(record *record) error {
	// 			offset, err := compactedSeg.Write(record)
	// 			if err == nil {
	// 				newIdx.Put(record.key, newIndex(compactedSegmentID, offset))
	// 			}
	// 			return nil
	// 		})
	// 	}
	// }

	// // 保存旧段引用以便稍后删除
	// oldSegments := db.segments

	// // 原子切换到新段和索引
	// db.segments = []*segment{compactedSeg}
	// db.idx = newIdx
	// db.mu.Unlock()

	// // 关闭并删除旧段文件（不需要锁定）
	// for _, seg := range oldSegments {
	// 	seg.Close()
	// 	os.Remove(seg.path)
	// }

	// log.Printf("Compaction complete: %d valid records, removed %d segments",
	// 	validRecords, len(oldSegments))

	return nil
}
