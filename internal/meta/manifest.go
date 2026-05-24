package meta

import (
	"fmt"
	"os"
	"path/filepath"
)

const currentFile = "CURRENT"
const TableMetaSize = 4 + 8 + 8 + 4 + 4 + 4

const (
	tagAddTable     = 1
	tagDeleteTable  = 2
	tagLastSequence = 3
	tagNextFileNum  = 4
	tagLogNum       = 5
)

func Open(dataDir string) (*VersionSet, error) {

	currentPath := filepath.Join(dataDir, currentFile)
	manifestName, err := os.ReadFile(currentPath)

	var manifestFile *os.File
	vs := &VersionSet{
		Current: &Version{
			Levels: make([][]*TableMeta, 7),
		},
	}

	if err != nil {
		if os.IsNotExist(err) {
			return newManifest(dataDir, vs)
		}
		return nil, err
	}

	manifestPath := filepath.Join(dataDir, string(manifestName))
	manifestFile, err = os.OpenFile(manifestPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if err := vs.replay(manifestFile); err != nil {
		return nil, err
	}

	vs.manifest = manifestFile
	return vs, nil

}

func newManifest(dataDir string, vs *VersionSet) (*VersionSet, error) {
	name := fmt.Sprintf("MANIFEST-%06d", 1)
	f, err := os.Create(filepath.Join(dataDir, name))
	if err != nil {
		return nil, err
	}

	err = os.WriteFile(filepath.Join(dataDir, currentFile), []byte(name), 0644)
	if err != nil {
		return nil, err
	}

	vs.manifest = f
	vs.nextFileNum = 1

	return vs, nil
}
