package meta

import (
	"encoding/json"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var objectsBucket = []byte("objects")

// Bolt is a durable Store backed by a bbolt file. Every write transaction is
// fsynced before it returns, so committed metadata survives a crash.
type Bolt struct {
	db *bolt.DB
}

var _ Store = (*Bolt)(nil)

// OpenBolt opens or creates the database at path. It fails after one second
// if another process holds the file.
func OpenBolt(path string) (*Bolt, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open metadata db %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(objectsBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Bolt{db: db}, nil
}

func (b *Bolt) Create(o Object) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(objectsBucket)
		if bk.Get([]byte(o.ID)) != nil {
			return ErrExists
		}
		return put(bk, o)
	})
}

func (b *Bolt) Get(id string) (Object, error) {
	var o Object
	err := b.db.View(func(tx *bolt.Tx) error {
		var err error
		o, err = get(tx.Bucket(objectsBucket), id)
		return err
	})
	return o, err
}

func (b *Bolt) Update(id string, fn func(*Object) error) (Object, error) {
	var o Object
	err := b.db.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(objectsBucket)
		var err error
		if o, err = get(bk, id); err != nil {
			return err
		}
		if err := fn(&o); err != nil {
			return err
		}
		o.ID = id
		return put(bk, o)
	})
	if err != nil {
		return Object{}, err
	}
	return o, nil
}

func (b *Bolt) Delete(id string) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(objectsBucket)
		if bk.Get([]byte(id)) == nil {
			return ErrNotFound
		}
		return bk.Delete([]byte(id))
	})
}

func (b *Bolt) List() ([]Object, error) {
	var out []Object
	err := b.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(objectsBucket).ForEach(func(k, v []byte) error {
			var o Object
			if err := json.Unmarshal(v, &o); err != nil {
				return fmt.Errorf("decode object %s: %w", k, err)
			}
			out = append(out, o)
			return nil
		})
	})
	return out, err // bbolt iterates keys in byte order, i.e. sorted by ID
}

func (b *Bolt) Close() error { return b.db.Close() }

func get(bk *bolt.Bucket, id string) (Object, error) {
	v := bk.Get([]byte(id))
	if v == nil {
		return Object{}, ErrNotFound
	}
	var o Object
	if err := json.Unmarshal(v, &o); err != nil {
		return Object{}, fmt.Errorf("decode object %s: %w", id, err)
	}
	return o, nil
}

func put(bk *bolt.Bucket, o Object) error {
	v, err := json.Marshal(o)
	if err != nil {
		return err
	}
	return bk.Put([]byte(o.ID), v)
}
