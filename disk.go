package accord

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

type stored struct {
	Term   uint64  `json:"term"`
	Vote   string  `json:"vote"`
	Commit uint64  `json:"commit"`
	Log    []Entry `json:"log"`
}

func loadState(dir string) (stored, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if errors.Is(err, os.ErrNotExist) {
		raw, err = os.ReadFile(filepath.Join(dir, "state.json.bak"))
	}
	if errors.Is(err, os.ErrNotExist) {
		return stored{Log: []Entry{{}}}, nil
	}
	if err != nil {
		return stored{}, err
	}
	var st stored
	if err := json.Unmarshal(raw, &st); err != nil {
		return stored{}, err
	}
	if len(st.Log) == 0 {
		st.Log = []Entry{{}}
	}
	return st, nil
}

// saveState replaces the previous file only after the new bytes are synced.
// A crash keeps state.json or state.json.bak, never a half-written file.
func saveState(dir string, st stored) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "state.tmp")
	dst := filepath.Join(dir, "state.json")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err = f.Write(raw); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	bak := dst + ".bak"
	_ = os.Remove(bak)
	if err = os.Rename(dst, bak); err != nil && !os.IsNotExist(err) {
		return err
	}
	var rename error
	for i := 0; i < 10; i++ {
		rename = os.Rename(tmp, dst)
		if rename == nil {
			_ = os.Remove(bak)
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = os.Rename(bak, dst)
	return rename
}
