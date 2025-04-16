package godb

// compactionStats 包含压缩操作的统计信息
type compactionStats struct {
	SegmentsTotal     int   // 总段数
	SegmentsCompacted int   // 压缩的段数
	SegmentsRemoved   int   // 被删除的段数
	RecordsTotal      int   // 总记录数
	RecordsLive       int   // 活跃的记录数
	RecordsRemoved    int   // 被删除的记录数
	BytesReclaimed    int64 // 释放的字节数
}
