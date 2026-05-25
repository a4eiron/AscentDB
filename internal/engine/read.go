package engine

import (
	"log"
	"math"
	"sort"

	"github.com/a4eiron/ascentdb/internal"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

func (e *Engine) Get(key string) ([]byte, bool) {
	if key == "" {
		return nil, false
	}
	return e.get(key, math.MaxUint64)
}

func (e *Engine) Scan(start, end string) *ScanIterator {
	return e.scan(start, end, math.MaxUint64)
}

func (e *Engine) scan(start, end string, seqNum uint64) *ScanIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var iters []internal.Iterator

	mtIter := e.mt.Iterator()
	mtIter.Seek(record.InternalKey{UserKey: start, SeqNum: seqNum})
	iters = append(iters, mtIter)

	if e.immt != nil {
		immtIter := e.immt.Iterator()
		immtIter.Seek(record.InternalKey{UserKey: start, SeqNum: seqNum})
		iters = append(iters, immtIter)
	}

	for _, tables := range e.vs.Current.Levels {
		for _, t := range tables {
			if t.MaxKey.UserKey < start || t.MinKey.UserKey > end {
				continue
			}

			reader, err := e.getReader(t)
			if err != nil {
				log.Fatalln(err)
			}

			iter := reader.Iterator()
			iter.Seek(record.InternalKey{UserKey: start, SeqNum: seqNum})
			iters = append(iters, iter)
		}
	}

	return NewScanIterator(iters, end, seqNum)
}

func (e *Engine) get(key string, seqNum uint64) ([]byte, bool) {
	lookupKey := record.InternalKey{
		UserKey: key,
		SeqNum:  seqNum,
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if val, found, deleted := e.mt.Get(key, lookupKey); found {
		if deleted {
			return nil, false
		}
		return val, true
	}

	if e.immt != nil {
		if val, found, deleted := e.immt.Get(key, lookupKey); found {
			if deleted {
				return nil, false
			}
			return val, true
		}
	}

	var best *record.Record

	checkTable := func(t *meta.TableMeta) {
		reader, err := e.getReader(t)
		if err != nil {
			log.Println(err)
			return
		}

		rec, ok, err := reader.Get(lookupKey)
		if err != nil {
			log.Println(err)
			return
		}
		if !ok {
			return
		}
		if best == nil || rec.SeqNum > best.SeqNum {
			best = rec
		}
	}

	for level := range e.vs.Current.Levels {
		tables := e.vs.Current.Levels[level]
		if level == 0 {
			for i := len(tables) - 1; i >= 0; i-- {
				t := tables[i]
				if key < t.MinKey.UserKey || key > t.MaxKey.UserKey {
					continue
				}
				checkTable(t)
			}
		} else {
			if len(tables) == 0 {
				continue
			}

			idx := sort.Search(len(tables), func(i int) bool {
				return tables[i].MaxKey.Compare(lookupKey) >= 0
			})

			if idx >= len(tables) {
				continue
			}
			checkTable(tables[idx])

			if best != nil {
				break
			}
		}

	}

	if best != nil {
		if best.Type == record.TypeDel {
			return nil, false
		}
		return best.Value, true
	}

	return nil, false
}

func (e *Engine) getReader(t *meta.TableMeta) (*sstable.TableReader, error) {
	if r, ok := e.tableCache.Load(t.FileNum); ok {
		return r.(*sstable.TableReader), nil
	}

	r, err := sstable.Open(e.tablePath(int(t.Level), t.FileNum), e.blockCache)
	if err != nil {
		return nil, err
	}
	actual, loaded := e.tableCache.LoadOrStore(t.FileNum, r)
	if loaded {
		r.Close()
		return actual.(*sstable.TableReader), nil
	}
	return r, nil
}
