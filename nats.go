package certmagic_nats

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
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
}

func init() {
	caddy.RegisterModule(Nats{})
}

func (n *Nats) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		var value string
		key := d.Val()

		if !d.Args(&value) {
			continue
		}

		switch key {
		case "hosts":
			n.Hosts = value
		case "bucket":
			n.Bucket = value
		case "creds":
			n.Creds = value
		case "inbox_prefix":
			n.InboxPrefix = value
		case "connection_name":
			n.ConnectionName = value
		}
	}

	return nil
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

func (n *Nats) Provision(ctx caddy.Context) error {
	n.logger = ctx.Logger(n)

	if n.InboxPrefix == "" {
		n.InboxPrefix = "_INBOX"
	}

	kv, err := connectNats(n.Hosts, n.Creds, n.Bucket, n.ConnectionName, n.InboxPrefix)
	if err != nil {
		return err
	}

	n.Client = kv
	return nil
}

func (Nats) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.nats",
		New: func() caddy.Module {
			return &Nats{}
		},
	}
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
	lockKey := fmt.Sprintf("LOCK.%s", key)
	var lastRevision uint64

	// Check for existing lock
	for {
		l, err := n.Client.Get(key)
		isErrNotExists := errors.Is(err, nats.ErrKeyNotFound)
		if err != nil && !isErrNotExists {
			return err
		}

		// if lock doesn't exist, break to create a new one
		if isErrNotExists {
			break
		}

		lastRevision = l.Revision()

		// Lock exists, check if expired or sleep 5 seconds and check again
		//expires, err := time.Parse(time.RFC3339, string(l.Value()))
		expires := time.Unix(0, int64(binary.LittleEndian.Uint64(l.Value())))
		if time.Now().After(expires) {
			if err := n.Unlock(ctx, key); err != nil {
				return err
			}
			break
		}

		select {
		case <-time.After(time.Duration(5 * time.Second)):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// lock doesn't exist, create it
	contents := make([]byte, 8)
	binary.LittleEndian.PutUint64(contents, uint64(time.Now().Add(time.Duration(5*time.Minute)).UnixNano()))

	if lastRevision > 0 {
		_, err := n.Client.Update(lockKey, contents, lastRevision)
		if err != nil {
			return err
		}
	}

	return n.Store(ctx, lockKey, contents)
}

// Unlock releases the lock for key. This method must ONLY be
// called after a successful call to Lock, and only after the
// critical section is finished, even if it errored or timed
// out. Unlock cleans up any resources allocated during Lock.
func (n *Nats) Unlock(ctx context.Context, key string) error {
	lockKey := fmt.Sprintf("LOCK.%s", key)
	return n.Delete(ctx, lockKey)
}

func (n *Nats) Store(ctx context.Context, key string, value []byte) error {
	n.logger.Info(fmt.Sprintf("Store: %v, %v bytes", key, len(value)))
	_, err := n.Client.Put(key, value)
	return err
}

func (n *Nats) Load(ctx context.Context, key string) ([]byte, error) {
	n.logger.Info(fmt.Sprintf("Load: %v", key))
	k, err := n.Client.Get(key)
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
	return n.Client.Delete(key)
}

func (n *Nats) Exists(ctx context.Context, key string) bool {
	n.logger.Info(fmt.Sprintf("Exists: %v", key))
	_, err := n.Client.Get(key)
	return err == nil
}

func (n *Nats) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	var keys []string

	k, err := n.Client.Keys()
	if err != nil {
		return keys, err
	}

	for _, v := range k {
		if strings.HasPrefix(v, prefix) {
			keys = append(keys, v)
		}
	}

	return keys, nil
}

func (n *Nats) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	n.logger.Info(fmt.Sprintf("Stat: %v", key))
	var ki certmagic.KeyInfo

	k, err := n.Client.Get(key)
	if err != nil {
		return ki, fs.ErrNotExist
	}

	ki.Key = key
	ki.Size = int64(len(k.Value()))
	ki.Modified = k.Created()
	ki.IsTerminal = true
	return ki, nil
}

// CertMagicStorage converts s to a certmagic.Storage instance.
func (n *Nats) CertMagicStorage() (certmagic.Storage, error) {
	return n, nil
}

var (
	_ caddy.Provisioner      = (*Nats)(nil)
	_ caddy.StorageConverter = (*Nats)(nil)
	_ caddyfile.Unmarshaler  = (*Nats)(nil)
)
