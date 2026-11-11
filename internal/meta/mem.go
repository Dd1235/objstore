package meta

import (
	"slices"
	"strings"
	"sync"
)

// Mem is an in-memory Store for tests and simulation.
type Mem struct {
	mu   sync.Mutex
	objs map[string]Object
}

var _ Store = (*Mem)(nil)

// NewMem returns an empty in-memory store.
func NewMem() *Mem { return &Mem{objs: make(map[string]Object)} }

func (m *Mem) Create(o Object) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[o.ID]; ok {
		return ErrExists
	}
	m.objs[o.ID] = o.Clone()
	return nil
}

func (m *Mem) Get(id string) (Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[id]
	if !ok {
		return Object{}, ErrNotFound
	}
	return o.Clone(), nil
}

func (m *Mem) Update(id string, fn func(*Object) error) (Object, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objs[id]
	if !ok {
		return Object{}, ErrNotFound
	}
	o = o.Clone()
	if err := fn(&o); err != nil {
		return Object{}, err
	}
	o.ID = id
	m.objs[id] = o.Clone()
	return o, nil
}

func (m *Mem) Delete(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.objs[id]; !ok {
		return ErrNotFound
	}
	delete(m.objs, id)
	return nil
}

func (m *Mem) List() ([]Object, error) {
	m.mu.Lock()
	out := make([]Object, 0, len(m.objs))
	for _, o := range m.objs {
		out = append(out, o.Clone())
	}
	m.mu.Unlock()
	slices.SortFunc(out, func(a, b Object) int { return strings.Compare(a.ID, b.ID) })
	return out, nil
}

func (m *Mem) Close() error { return nil }
