package cos

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/fsnotify/fsnotify"
	_ "modernc.org/sqlite"
)

type Task struct {
	Ticket   string `json:"ticket"`
	Priority string `json:"priority"`
	Repo     string `json:"repo"`
	Outcome  string `json:"outcome"`
	Status   string `json:"status"`
	Host     string `json:"host"`
	Model    string `json:"model"`
	ModelWhy string `json:"modelWhy"`
	Why      string `json:"why"`
	PR       string `json:"pr"`
	Next     string `json:"next"`
	Stale    bool   `json:"stale,omitempty"`
}
type Note struct {
	ID   int64  `json:"id"`
	Kind string `json:"kind"`
	Text string `json:"text"`
}
type Story struct {
	Direction string `json:"direction"`
	Notes     []Note `json:"notes"`
	Tasks     []Task `json:"tasks"`
}
type Live struct {
	At     string            `json:"at"`
	Fleet  []HostFleet       `json:"fleet"`
	PRs    []PR              `json:"prs"`
	Issues []Issue           `json:"issues"`
	Asks   []Ask             `json:"asks"`
	Errors map[string]string `json:"errors"`
}
type Store struct {
	DB   *sql.DB
	Path string
}

func OpenStore(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = f.Close(); err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA busy_timeout=5000; PRAGMA journal_mode=WAL;
	CREATE TABLE IF NOT EXISTS tasks(ticket TEXT PRIMARY KEY, payload TEXT NOT NULL);
	CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, kind TEXT NOT NULL, text TEXT NOT NULL);
	CREATE TABLE IF NOT EXISTS live(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
	CREATE TABLE IF NOT EXISTS autopilot(id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL);
	CREATE TABLE IF NOT EXISTS autopilot_log(seq INTEGER PRIMARY KEY, payload TEXT NOT NULL);
	CREATE TABLE IF NOT EXISTS revision(id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL);
	INSERT OR IGNORE INTO revision VALUES(1,0);`)
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db, path}, nil
}
func (s *Store) Close() error { return s.DB.Close() }
func (s *Store) change(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE revision SET value=value+1 WHERE id=1`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// WAL notifications can precede commit visibility; this wake-up follows it.
	if err := os.WriteFile(s.Path+".notify", []byte{0}, 0600); err != nil {
		return fmt.Errorf("state committed but change notification failed: %w", err)
	}
	return nil
}
func (s *Store) Revision(ctx context.Context) (int64, error) {
	var n int64
	err := s.DB.QueryRowContext(ctx, `SELECT value FROM revision WHERE id=1`).Scan(&n)
	return n, err
}

func completed(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "done", "merged", "completed", "closed", "cancelled", "canceled", "dropped", "superseded", "archived":
		return true
	}
	return false
}
func priorityRank(p string) int {
	switch p {
	case "FULL SEND":
		return -1
	case "P0":
		return 0
	case "P1":
		return 1
	case "P2":
		return 2
	case "P3":
		return 3
	}
	return 9
}

func (s *Store) SetTask(ctx context.Context, patch map[string]any) (Task, error) {
	data, err := json.Marshal(patch)
	if err != nil {
		return Task{}, err
	}
	var input Task
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&input); err != nil {
		return Task{}, err
	}
	for key := range patch {
		switch key {
		case "ticket", "why", "modelWhy", "next":
		default:
			return Task{}, fmt.Errorf("%s is derived from live sources; task set accepts ticket, why, modelWhy, next", key)
		}
	}
	input.Ticket = strings.ToUpper(strings.TrimSpace(input.Ticket))
	if input.Ticket == "" {
		return Task{}, errors.New("task set requires ticket")
	}
	patch["ticket"] = input.Ticket
	var result Task
	err = s.change(ctx, func(tx *sql.Tx) error {
		old := map[string]any{}
		var raw string
		err := tx.QueryRowContext(ctx, `SELECT payload FROM tasks WHERE ticket=?`, input.Ticket).Scan(&raw)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			if err = json.Unmarshal([]byte(raw), &old); err != nil {
				return err
			}
		}
		for k, v := range patch {
			old[k] = v
		}
		data, err := json.Marshal(old)
		if err != nil {
			return err
		}
		if err = json.Unmarshal(data, &result); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO tasks VALUES(?,?) ON CONFLICT(ticket) DO UPDATE SET payload=excluded.payload`, input.Ticket, string(data))
		return err
	})
	return result, err
}
func (s *Store) RemoveTask(ctx context.Context, ticket string) error {
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE ticket=?`, strings.ToUpper(ticket))
		return err
	})
}

func validNote(kind string) bool {
	switch kind {
	case "direction", "missing", "suggest", "need", "decide":
		return true
	}
	return false
}
func (s *Store) AddNote(ctx context.Context, kind, text string) (Note, error) {
	if !validNote(kind) {
		return Note{}, errors.New("note kind must be direction, missing, suggest, need, or decide")
	}
	if strings.TrimSpace(text) == "" {
		return Note{}, errors.New("note text is empty")
	}
	var id int64
	err := s.change(ctx, func(tx *sql.Tx) error {
		if kind == "direction" {
			if _, err := tx.ExecContext(ctx, `DELETE FROM notes WHERE kind='direction'`); err != nil {
				return err
			}
		}
		r, err := tx.ExecContext(ctx, `INSERT INTO notes(kind,text) VALUES(?,?)`, kind, text)
		if err != nil {
			return err
		}
		id, err = r.LastInsertId()
		return err
	})
	return Note{id, kind, text}, err
}
func (s *Store) RemoveNote(ctx context.Context, kind string, id int64) error {
	if !validNote(kind) {
		return errors.New("invalid note kind")
	}
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM notes WHERE kind=? AND id=?`, kind, id)
		return err
	})
}
func (s *Store) ClearNotes(ctx context.Context, kind string) error {
	if !validNote(kind) {
		return errors.New("invalid note kind")
	}
	return s.change(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `DELETE FROM notes WHERE kind=?`, kind)
		return err
	})
}
func (s *Store) Story(ctx context.Context) (Story, error) {
	out := Story{Notes: []Note{}, Tasks: []Task{}}
	rows, err := s.DB.QueryContext(ctx, `SELECT id,kind,text FROM notes ORDER BY id`)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var n Note
		if err := rows.Scan(&n.ID, &n.Kind, &n.Text); err != nil {
			rows.Close()
			return out, err
		}
		if n.Kind == "direction" {
			out.Direction = n.Text
		} else {
			out.Notes = append(out.Notes, n)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	rows, err = s.DB.QueryContext(ctx, `SELECT payload FROM tasks ORDER BY ticket`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		var t Task
		if err := rows.Scan(&raw); err != nil {
			return out, err
		}
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			return out, err
		}
		if !completed(t.Status) {
			out.Tasks = append(out.Tasks, t)
		}
	}
	return out, rows.Err()
}
func (s *Store) TaskPatches(ctx context.Context) (map[string]map[string]any, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT ticket,payload FROM tasks`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[string]any{}
	for rows.Next() {
		var ticket, raw string
		if err := rows.Scan(&ticket, &raw); err != nil {
			return nil, err
		}
		var p map[string]any
		if err := json.Unmarshal([]byte(raw), &p); err != nil {
			return nil, err
		}
		out[ticket] = p
	}
	return out, rows.Err()
}
func (s *Store) Live(ctx context.Context) (Live, error) {
	out := Live{Fleet: []HostFleet{}, PRs: []PR{}, Issues: []Issue{}, Asks: []Ask{}, Errors: map[string]string{}}
	var raw string
	err := s.DB.QueryRowContext(ctx, `SELECT payload FROM live WHERE id=1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	err = json.Unmarshal([]byte(raw), &out)
	return out, err
}
func (s *Store) SaveLive(ctx context.Context, live Live) error {
	old, err := s.Live(ctx)
	if err != nil {
		return err
	}
	data, err := json.Marshal(live)
	if err != nil {
		return err
	}
	return s.change(ctx, func(tx *sql.Tx) error {
		if live.Errors["ku"] == "" {
			current := map[string]bool{}
			for _, i := range live.Issues {
				current[i.Identifier] = true
			}
			for _, i := range old.Issues {
				if !current[i.Identifier] {
					if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE ticket=?`, i.Identifier); err != nil {
						return err
					}
				}
			}
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO live VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload`, string(data))
		return err
	})
}

// Watch the WAL too: a CLI process committing to SQLite immediately wakes SSE.
func (s *Store) Watch(ctx context.Context) (<-chan struct{}, <-chan error, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, nil, err
	}
	if err = w.Add(filepath.Dir(s.Path)); err != nil {
		w.Close()
		return nil, nil, err
	}
	changes := make(chan struct{}, 1)
	errs := make(chan error, 1)
	go func() {
		defer w.Close()
		defer close(changes)
		defer close(errs)
		for {
			select {
			case <-ctx.Done():
				return
			case err, ok := <-w.Errors:
				if !ok {
					return
				}
				select {
				case errs <- fmt.Errorf("state watcher: %w", err):
				default:
				}
			case e, ok := <-w.Events:
				if !ok {
					return
				}
				if e.Name == s.Path || e.Name == s.Path+"-wal" || e.Name == s.Path+".notify" {
					select {
					case changes <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return changes, errs, nil
}
