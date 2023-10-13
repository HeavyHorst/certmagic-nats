package certmagic_nats

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math/rand"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

type Nats struct {
	logger *zap.Logger
	Client nats.KeyValue

	Hosts          string `json:"hosts"`
	Bucket         string `json:"bucket"`
	Creds          string `json:"creds"`
	InboxPrefix    string `json:"inbox_prefix"`
	ConnectionName string `json:"connection_name"`

	revMap  map[string]uint64
	maplock sync.Mutex
}

var (
	_ caddy.Provisioner = (*Nats)(nil)
	_ certmagic.Storage = (*Nats)(nil)
	_ certmagic.Locker  = (*Nats)(nil)
)

// should be save to use as it is not allowed to be used in urls
const replaceChar = "#"

func normalizeNatsKey(key string) string {
	if len(key) == 0 {
		return key
	}

	key = strings.ReplaceAll(key, ".", replaceChar)
	key = strings.ReplaceAll(key, "/", ".")
	key = strings.ReplaceAll(key, replaceChar, "/")
	return key
}

func denormalizeNatsKey(key string) string {
	key = strings.ReplaceAll(key, "/", replaceChar)
	key = strings.ReplaceAll(key, ".", "/")
	key = strings.ReplaceAll(key, replaceChar, ".")
	return key
}

func connectNats(host, creds, bucket, connectionName, inboxPrefix string) (nats.KeyValue, error) {
	options := []nats.Option{nats.Name(connectionName), nats.CustomInboxPrefix(inboxPrefix)}
	if creds != "" {
		options = append(options, nats.UserCredentials(creds))
	}

	nc, err := nats.Connect(host, options...)
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, err
	}

	return js.KeyValue(bucket)
}

func (n *Nats) setRev(key string, value uint64) {
	n.maplock.Lock()
	defer n.maplock.Unlock()
	n.revMap[key] = value
}

func (n *Nats) getRev(key string) uint64 {
	n.maplock.Lock()
	defer n.maplock.Unlock()
	return n.revMap[key]
}

func isWrongSequence(err error) bool {
	return strings.Contains(err.Error(), "wrong last sequence")
}

// Lock acquires the lock for key, blocking until the lock
// can be obtained or an error is returned. Note that, even
// after acquiring a lock, an idempotent operation may have
// already been performed by another process that acquired
// the lock before - so always check to make sure idempotent
// operations still need to be performed after acquiring the
// lock.
//
// The actual implementation of obtaining of a lock must be
// an atomic operation so that multiple Lock calls at the
// same time always results in only one caller receiving the
// lock at any given time.
//
// To prevent deadlocks, all implementations (where this concern
// is relevant) should put a reasonable expiration on the lock in
// case Unlock is unable to be called due to some sort of network
// failure or system crash.
func (n *Nats) Lock(ctx context.Context, key string) error {
	n.logger.Info(fmt.Sprintf("Lock: %v", key))
	lockKey := fmt.Sprintf("LOCK.%s", key)

loop:
	for {
		// Check for existing lock
		revision, err := n.Client.Get(lockKey)
		if err != nil && !errors.Is(err, nats.ErrKeyNotFound) {
			return err
		}

		if revision == nil {
			break
		}

		expires := time.Unix(0, int64(binary.LittleEndian.Uint64(revision.Value())))
		// Lock exists, check if expired
		if time.Now().After(expires) {
			// the lock expired and can be deleted
			// break and try to create a new one
			n.setRev(lockKey, revision.Revision())
			if err := n.Unlock(ctx, key); err != nil {
				if isWrongSequence(err) {
					goto loop
				}
				return err
			}
			break
		}

		select {
		// retry after a short period of time
		case <-time.After(time.Duration(50+rand.Float64()*(200-50+1)) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// lock doesn't exist, create it
	contents := make([]byte, 8)
	binary.LittleEndian.PutUint64(contents, uint64(time.Now().Add(time.Duration(5*time.Minute)).UnixNano()))
	nrev, err := n.Client.Create(lockKey, contents)
	if err != nil && isWrongSequence(err) {
		// another process created the lock in the meantime
		// try again
		goto loop
	}

	if err != nil {
		return err
	}

	n.setRev(lockKey, nrev)
	return nil
}

// Unlock releases the lock for key. This method must ONLY be
// called after a successful call to Lock, and only after the
// critical section is finished, even if it errored or timed
// out. Unlock cleans up any resources allocated during Lock.
func (n *Nats) Unlock(ctx context.Context, key string) error {
	n.logger.Info(fmt.Sprintf("Unlock: %v", key))
	lockKey := fmt.Sprintf("LOCK.%s", key)
	return n.Client.Delete(lockKey, nats.LastRevision(n.getRev(lockKey)))
}

func (n *Nats) Store(ctx context.Context, key string, value []byte) error {
	n.logger.Info(fmt.Sprintf("Store: %v, %v bytes", key, len(value)))
	_, err := n.Client.Put(normalizeNatsKey(key), value)
	return err
}

func (n *Nats) Load(ctx context.Context, key string) ([]byte, error) {
	n.logger.Info(fmt.Sprintf("Load: %v", key))
	k, err := n.Client.Get(normalizeNatsKey(key))
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, fs.ErrNotExist
		}

		return nil, err
	}

	return k.Value(), nil
}

func (n *Nats) Delete(ctx context.Context, key string) error {
	n.logger.Info(fmt.Sprintf("Delete: %v", key))
	return n.Client.Delete(normalizeNatsKey(key))
}

func (n *Nats) Exists(ctx context.Context, key string) bool {
	n.logger.Info(fmt.Sprintf("Exists: %v", key))
	_, err := n.Client.Get(normalizeNatsKey(key))
	return err == nil
}

func (n *Nats) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	n.logger.Info(fmt.Sprintf("List: %v, %v", prefix, recursive))
	oprefix := strings.TrimSuffix(prefix, "/")
	prefix = normalizeNatsKey(prefix)

	if len(prefix) > 1 && prefix[len(prefix)-1] != '.' {
		prefix += "."
	}

	prefix += ">"

	fmt.Println(prefix)

	watcher, err := n.Client.Watch(prefix, nats.IgnoreDeletes(), nats.MetaOnly(), nats.Context(ctx))
	if err != nil {
		return nil, err
	}
	defer watcher.Stop()

	var keys []string
	for entry := range watcher.Updates() {
		if entry == nil {
			fmt.Println("WAAAAU")
			break
		}

		keys = append(keys, entry.Key())
	}

	for k := range keys {
		keys[k] = denormalizeNatsKey(keys[k])
	}

	if recursive {
		return keys, nil
	}

	dirs := make(map[string]struct{})
	for k := range keys {
		paths := strings.Split(keys[k], "/")
		var dir string
		for i := range paths {
			dir += paths[i]
			if path.Dir(dir) == oprefix {
				dirs[dir] = struct{}{}
			}
			dir += "/"
		}
	}

	dkeys := make([]string, 0, len(dirs))
	for k := range dirs {
		dkeys = append(dkeys, k)
	}

	return dkeys, nil
}

func (n *Nats) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	n.logger.Info(fmt.Sprintf("Stat: %v", key))
	var ki certmagic.KeyInfo

	key = strings.TrimSuffix(key, "/")
	nkey := normalizeNatsKey(key)
	k, err := n.Client.Get(nkey)
	if err == nats.ErrKeyNotFound {
		entries, err := n.List(ctx, nkey, false)
		if err != nil {
			return ki, fs.ErrNotExist
		}

		// the key doesn't exists but has children
		// so it's a directory
		if len(entries) > 0 {
			ki.IsTerminal = false
			ki.Key = key
			return ki, nil
		}

		return ki, fs.ErrNotExist
	} else if err != nil {
		return ki, fs.ErrNotExist
	}

	ki.Key = key
	ki.Size = int64(len(k.Value()))
	ki.Modified = k.Created()
	ki.IsTerminal = true
	return ki, nil
}
