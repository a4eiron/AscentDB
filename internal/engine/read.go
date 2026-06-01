package engine

import (
	"bytes"
	"log"
	"math"
	"sort"

	"github.com/a4eiron/ascentdb/internal"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

func (e *Engine) Get(key []byte) ([]byte, bool) {
	if key == nil {
		return nil, false
	}
	return e.get(key, math.MaxUint64)
}

func (e *Engine) Scan(start, end []byte) *ScanIterator {
	return e.scan(start, end, math.MaxUint64)
}

func (e *Engine) scan(start, end []byte, seqNum uint64) *ScanIterator {
	e.mu.RLock()

	mt := e.mt
	immt := e.immt
	mt.Ref()
	if immt != nil {
		immt.Ref()
	}

	e.mu.RUnlock()

	var iters []internal.Iterator

	mtIter := mt.Iterator()
	mtIter.Seek(record.InternalKey{UserKey: start, SeqNum: seqNum})
	iters = append(iters, mtIter)

	if immt != nil {
		immtIter := immt.Iterator()
		immtIter.Seek(record.InternalKey{UserKey: start, SeqNum: seqNum})
		iters = append(iters, immtIter)
	}

	for _, tables := range e.vs.Current.Levels {
		for _, t := range tables {
			if bytes.Compare(t.MaxKey.UserKey, start) < 0 || bytes.Compare(t.MinKey.UserKey, end) > 0 {
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

	return NewScanIterator(iters, end, seqNum, func() {
		mt.Unref()
		if immt != nil {
			immt.Unref()
		}
	})
}

func (e *Engine) get(key []byte, seqNum uint64) ([]byte, bool) {
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

	var (
		best      record.Record
		bestFound bool
	)

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
		if !bestFound || rec.SeqNum > best.SeqNum {
			best = rec
			bestFound = true
		}
	}

	for level := range e.vs.Current.Levels {
		tables := e.vs.Current.Levels[level]
		if level == 0 {
			for i := len(tables) - 1; i >= 0; i-- {
				t := tables[i]
				if bytes.Compare(key, t.MinKey.UserKey) < 0 || bytes.Compare(key, t.MaxKey.UserKey) > 0 {
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

			if bestFound {
				break
			}
		}

	}

	if bestFound {
		if best.Type == record.TypeDel {
			return nil, false
		}
		return best.Value, true
	}

	return nil, false
}

func (e *Engine) getReader(t *meta.TableMeta) (*sstable.TableReader, error) {
	e.tableCacheMu.RLock()
	r, ok := e.tableCache[t.FileNum]
	e.tableCacheMu.RUnlock()
	if ok {
		return r, nil
	}

	r, err := sstable.Open(e.tablePath(int(t.Level), t.FileNum), e.blockCache)
	if err != nil {
		return nil, err
	}

	e.tableCacheMu.Lock()
	if existing, ok := e.tableCache[t.FileNum]; ok {
		e.tableCacheMu.Unlock()
		r.Close()
		return existing, nil
	}
	e.tableCache[t.FileNum] = r
	e.tableCacheMu.Unlock()
	return r, nil
}
