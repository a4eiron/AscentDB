package engine

import (
	"fmt"
	"os"
	"path/filepath"
)

func (e *Engine) tablePath(level int, fileNum uint64) string {
	dir := filepath.Join(e.opts.DataDir, "tables", fmt.Sprintf("L%d", level))
	return filepath.Join(dir, fmt.Sprintf("table-%06d.sst", fileNum))
}

func (e *Engine) ensureLevelDir(level int) error {
	dir := filepath.Join(e.opts.DataDir, "tables", fmt.Sprintf("L%d", level))
	return os.MkdirAll(dir, 0755)
}

func (e *Engine) walPath(fileNum uint64) string {
	dir := filepath.Join(e.opts.DataDir, "wal")
	return filepath.Join(dir, fmt.Sprintf("wal-%06d.log", fileNum))
}
