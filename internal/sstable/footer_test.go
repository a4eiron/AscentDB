package sstable

import "testing"

func TestFooterCorruption(t *testing.T) {
	f := Footer{
		IndexOffset: 100,
		IndexSize:   200,
		Magic:       Magic,
	}

	b := encodeFooter(f)

	// corrupt magic
	b[len(b)-1] = 0x00

	_, err := decodeFooter(b)

	if err == nil {
		t.Fatal("expected corruption error")
	}
}
