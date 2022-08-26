package certmagic_nats

import (
	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
)

var (
	_ caddy.StorageConverter = (*Nats)(nil)
	_ caddyfile.Unmarshaler  = (*Nats)(nil)
)

func init() {
	caddy.RegisterModule(Nats{})
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

	n.revMap = make(map[string]uint64)

	n.Client = kv
	return nil
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

func (Nats) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.nats",
		New: func() caddy.Module {
			return &Nats{}
		},
	}
}

// CertMagicStorage converts s to a certmagic.Storage instance.
func (n *Nats) CertMagicStorage() (certmagic.Storage, error) {
	return n, nil
}
