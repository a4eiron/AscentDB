package meta

func FindOverlapping(tables []*TableMeta, minKey, maxKey string) []*TableMeta {
	var out []*TableMeta

	for _, t := range tables {
		if t.MaxKey.UserKey < minKey || t.MinKey.UserKey > maxKey {
			continue
		}

		out = append(out, t)
	}
	return out
}

// spans multiple tables
func KeyRangeOf(tables []*TableMeta) (string, string) {
	if len(tables) == 0 {
		return "", ""
	}

	minKey := tables[0].MinKey.UserKey
	maxKey := tables[0].MaxKey.UserKey

	for _, t := range tables[1:] {
		if t.MinKey.UserKey < minKey {
			minKey = t.MinKey.UserKey
		}

		if t.MaxKey.UserKey > maxKey {
			maxKey = t.MaxKey.UserKey
		}
	}
	return minKey, maxKey
}
