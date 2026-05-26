package meta

import "bytes"

func FindOverlapping(tables []*TableMeta, minKey, maxKey []byte) []*TableMeta {
	var out []*TableMeta

	for _, t := range tables {
		if bytes.Compare(t.MaxKey.UserKey, minKey) < 0 || bytes.Compare(t.MinKey.UserKey, maxKey) > 0 {
			continue
		}

		out = append(out, t)
	}
	return out
}

// spans multiple tables
func KeyRangeOf(tables []*TableMeta) ([]byte, []byte) {
	if len(tables) == 0 {
		return nil, nil
	}

	minKey := tables[0].MinKey.UserKey
	maxKey := tables[0].MaxKey.UserKey

	for _, t := range tables[1:] {
		if bytes.Compare(t.MinKey.UserKey, minKey) < 0 {
			minKey = t.MinKey.UserKey
		}

		if bytes.Compare(t.MaxKey.UserKey, maxKey) > 0 {
			maxKey = t.MaxKey.UserKey
		}
	}
	return minKey, maxKey
}
