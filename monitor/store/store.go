// Package store persists the map durably (write, fsync, rename).
package store

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	"mycelium/core/mapmodel"
)

type Store struct {
	path string
}

type file struct {
	Map     *mapmodel.Map `json:"map"`
	Applied uint64        `json:"applied"`
}

func Open(path string) *Store {
	return &Store{path: path}
}

// Load returns the persisted map (nil if none) and the Raft index it reflects.
func (s *Store) Load() (m *mapmodel.Map, applied uint64, err error) {
	b, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var f file
	if err := json.Unmarshal(b, &f); err != nil {
		return nil, 0, err
	}
	return f.Map, f.Applied, nil
}

// Save writes a temp file, fsyncs it, renames it over the old one and fsyncs the directory.
// applied is the Raft index m reflects (0 in single-instance mode).
func (s *Store) Save(m *mapmodel.Map, applied uint64) error {
	b, err := json.Marshal(file{Map: m, Applied: applied})
	if err != nil {
		return err
	}
	return WriteFile(s.path, b)
}

// WriteFile durably replaces path with data: temp file, fsync, rename, fsync of the directory.
func WriteFile(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
