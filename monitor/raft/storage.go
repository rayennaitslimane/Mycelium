package raft

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"mycelium/core/wire"
	"mycelium/monitor/store"
)

// hardState is persisted before answering any vote or append.
type hardState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// storage keeps the hard state and the log (JSON lines) in dir, fsynced.
type storage struct {
	dir string
	log *os.File // opened for append
}

func (s *storage) hardStatePath() string { return filepath.Join(s.dir, "hardstate.json") }
func (s *storage) logPath() string       { return filepath.Join(s.dir, "log.jsonl") }

func openStorage(dir string) (*storage, hardState, []wire.Entry, error) {
	s := &storage{dir: dir}
	var hs hardState
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, hs, nil, err
	}
	b, err := os.ReadFile(s.hardStatePath())
	switch {
	case err == nil:
		if err := json.Unmarshal(b, &hs); err != nil {
			return nil, hs, nil, err
		}
	case !errors.Is(err, fs.ErrNotExist):
		return nil, hs, nil, err
	}

	b, err = os.ReadFile(s.logPath())
	missing := errors.Is(err, fs.ErrNotExist)
	if err != nil && !missing {
		return nil, hs, nil, err
	}
	// A trailing partial line is a torn append that was never acknowledged: drop it.
	complete := b[:bytes.LastIndexByte(b, '\n')+1]
	var es []wire.Entry
	for _, line := range bytes.Split(complete, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var e wire.Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, hs, nil, fmt.Errorf("raft log: %w", err)
		}
		if e.Index != uint64(len(es))+1 {
			return nil, hs, nil, fmt.Errorf("raft log: entry %d at position %d", e.Index, len(es)+1)
		}
		es = append(es, e)
	}
	if missing || len(complete) != len(b) {
		if err := store.WriteFile(s.logPath(), complete); err != nil {
			return nil, hs, nil, err
		}
	}
	if s.log, err = os.OpenFile(s.logPath(), os.O_WRONLY|os.O_APPEND, 0); err != nil {
		return nil, hs, nil, err
	}
	return s, hs, es, nil
}

func (s *storage) saveHardState(hs hardState) error {
	b, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	return store.WriteFile(s.hardStatePath(), b)
}

func (s *storage) append(es []wire.Entry) error {
	b, err := encode(es)
	if err != nil {
		return err
	}
	if _, err := s.log.Write(b); err != nil {
		return err
	}
	return s.log.Sync()
}

// rewrite replaces the whole log with es; used to truncate a conflicting suffix.
func (s *storage) rewrite(es []wire.Entry) error {
	b, err := encode(es)
	if err != nil {
		return err
	}
	if err := s.log.Close(); err != nil {
		return err
	}
	if err := store.WriteFile(s.logPath(), b); err != nil {
		return err
	}
	s.log, err = os.OpenFile(s.logPath(), os.O_WRONLY|os.O_APPEND, 0)
	return err
}

func encode(es []wire.Entry) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf) // Encode appends '\n'
	for _, e := range es {
		if err := enc.Encode(e); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}
