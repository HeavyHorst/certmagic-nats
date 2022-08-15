package certmagic_nats

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func startNats() {
	opts := &server.Options{
		JetStream: true,
	}

	// Initialize new server with options
	ns, err := server.NewServer(opts)

	if err != nil {
		panic(err)
	}

	// Start the server via goroutine
	go ns.Start()

	// Wait for server to be ready for connections
	if !ns.ReadyForConnections(4 * time.Second) {
		panic("not ready for connection")
	}
}

func init() {
	startNats()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}

	js, err := nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		panic(err)
	}

	buckets := []string{"stat", "test"}
	for _, bucket := range buckets {
		_, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  bucket,
			Storage: nats.MemoryStorage,
		})
		if err != nil {
			panic(err)
		}
	}
}

func TestNats_Stat(t *testing.T) {
	type fields struct {
		logger *zap.Logger
		Client nats.KeyValue
		Hosts  string
		Bucket string
	}
	type args struct {
		ctx context.Context
		key string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		want     certmagic.KeyInfo
		wantErr  bool
		initFunc func(n *Nats)
	}{
		{
			name: "test key not exists",
			fields: fields{
				logger: zap.NewNop(),
				Hosts:  nats.DefaultURL,
				Bucket: "stat",
			},
			args: args{
				ctx: context.Background(),
				key: "test1",
			},
			want:    certmagic.KeyInfo{},
			wantErr: true,
		},
		{
			name: "test key exists",
			fields: fields{
				logger: zap.NewNop(),
				Hosts:  nats.DefaultURL,
				Bucket: "stat",
			},
			args: args{
				ctx: context.Background(),
				key: "test2",
			},
			want: certmagic.KeyInfo{
				Key:        "test2",
				Size:       int64(len("test2")),
				IsTerminal: true,
			},
			initFunc: func(n *Nats) {
				n.Client.PutString("test2", "test2")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Nats{
				logger: tt.fields.logger,
				Client: tt.fields.Client,
				Hosts:  tt.fields.Hosts,
				Bucket: tt.fields.Bucket,
			}

			err := n.Provision(caddy.Context{})
			if err != nil {
				t.Errorf("Provision() error = %v", err)
				return
			}

			if tt.initFunc != nil {
				tt.initFunc(n)
			}

			got, err := n.Stat(tt.args.ctx, tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Nats.Stat() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got.Modified, tt.want.Modified) && time.Since(got.Modified) > time.Second {
				t.Errorf("Modified time too old: %v", got.Modified)
			}

			got.Modified = time.Time{}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Nats.Stat() = %v, want %v", got, tt.want)
			}
		})
	}
}
