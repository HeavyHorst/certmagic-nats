package certmagic_nats

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/fs"
	"path"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/certmagic"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var started bool
var startedLock sync.Mutex

func getTestData() (string, string, string, []string) {
	crt := path.Join("acme", "example.com", "sites", "example.com", "example.com.crt")
	key := path.Join("acme", "example.com", "sites", "example.com", "example.com.key")
	js := path.Join("acme", "example.com", "sites", "example.com", "example.com.json")
	want := []string{crt, key, js}
	sort.Strings(want)
	return crt, key, js, want
}

func startNatsServer() {
	startedLock.Lock()
	defer startedLock.Unlock()
	if started {
		return
	}

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

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}

	js, err := nc.JetStream()
	if err != nil {
		panic(err)
	}

	buckets := []string{"stat", "basic", "list", "listnr"}
	for _, bucket := range buckets {
		_, err = js.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  bucket,
			History: 1,
			Storage: nats.MemoryStorage,
		})
		if err != nil {
			panic(err)
		}
	}

	started = true
}

func getNatsClient(bucket string) *Nats {
	startNatsServer()

	n := &Nats{
		Hosts:  nats.DefaultURL,
		Bucket: bucket,
	}
	n.Provision(caddy.Context{})
	n.logger = zap.NewNop()
	return n
}

func TestNats_Stat(t *testing.T) {
	n := getNatsClient("stat")

	data := make([]byte, 50)
	rand.Read(data)

	storeValues := []string{"testStat1", "testFolder/testStat2", "testFolder/testStat3"}
	for _, sv := range storeValues {
		err := n.Store(context.Background(), sv, data)
		if err != nil {
			t.Fatalf("Store() error = %v", err)
		}
	}

	testStat := func(key string, want certmagic.KeyInfo, wantErr error) {
		got, err := n.Stat(context.Background(), key)
		if err != wantErr {
			t.Errorf("Stat() error = %v", err)
		}

		if got.IsTerminal {
			if !reflect.DeepEqual(got.Modified, want) && time.Since(got.Modified) > time.Second {
				t.Errorf("Modified time too old: %v", got.Modified)
			}
		}

		got.Modified = time.Time{}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Nats.Stat() = %v, want %v", got, want)
		}
	}

	key := "testStat1"
	want := certmagic.KeyInfo{Key: key, Size: 50, IsTerminal: true}
	testStat(key, want, nil)

	key = "testFolder/testStat2"
	want = certmagic.KeyInfo{Key: key, Size: 50, IsTerminal: true}
	testStat(key, want, nil)

	key = "testFolder/testStat3"
	want = certmagic.KeyInfo{Key: key, Size: 50, IsTerminal: true}
	testStat(key, want, nil)

	key = "testFolder"
	want = certmagic.KeyInfo{Key: key, IsTerminal: false}
	testStat(key, want, nil)

	key = "testFolder/"
	want = certmagic.KeyInfo{Key: "testFolder", IsTerminal: false}
	testStat(key, want, nil)

	key = "testFolder/testStat4"
	want = certmagic.KeyInfo{}
	testStat(key, want, fs.ErrNotExist)
}

func TestNats_StatKeyNotExists(t *testing.T) {
	n := getNatsClient("stat")
	got, err := n.Stat(context.Background(), "testStatNotExistingKey")
	if err != fs.ErrNotExist {
		t.Errorf("Stat() error = %v, want %v", err, fs.ErrNotExist)
	}

	if !reflect.DeepEqual(got, certmagic.KeyInfo{}) {
		t.Errorf("Stat() got = %v, want empty", got)
	}
}

func TestNats_StoreLoad(t *testing.T) {
	n := getNatsClient("basic")

	data := make([]byte, 50)
	rand.Read(data)

	err := n.Store(context.Background(), "test1", data)
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	got, err := n.Load(context.Background(), "test1")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("Load() got = %v, want %v", got, data)
	}
}

func TestNats_LoadKeyNotExists(t *testing.T) {
	n := getNatsClient("basic")

	got, err := n.Load(context.Background(), "NotExistingKey")
	if got != nil {
		t.Errorf("Load() got = %v, want nil", got)
	}

	if err != fs.ErrNotExist {
		t.Errorf("Load() error = %v, want %v", err, fs.ErrNotExist)
	}
}

func TestNats_Delete(t *testing.T) {
	n := getNatsClient("basic")

	err := n.Store(context.Background(), "testDelete", []byte("delete"))
	if err != nil {
		t.Fatalf("Store() error = %v", err)
	}

	err = n.Delete(context.Background(), "testDelete")
	if err != nil {
		t.Errorf("Delete() error = %v", err)
	}

	got, err := n.Load(context.Background(), "testDelete")
	if got != nil {
		t.Errorf("Load() got = %v, want nil", got)
	}

	if err != fs.ErrNotExist {
		t.Errorf("Load() error = %v, want %v", err, fs.ErrNotExist)
	}
}

func TestNats_List(t *testing.T) {
	n := getNatsClient("list")

	crt, key, js, want := getTestData()

	n.Store(context.Background(), crt, []byte("crt"))
	n.Store(context.Background(), key, []byte("key"))
	n.Store(context.Background(), js, []byte("meta"))

	matches := []string{path.Dir(crt), path.Dir(crt) + "/", ""}

	for _, v := range matches {
		keys, err := n.List(context.Background(), v, true)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		sort.Strings(keys)
		if !reflect.DeepEqual(keys, want) {
			t.Errorf("List() got = %v, want %v", keys, want)
		}
	}
}

func TestNats_ListNonRecursive(t *testing.T) {
	n := getNatsClient("listnr")

	crt, key, js, owant := getTestData()

	n.Store(context.Background(), crt, []byte("crt"))
	n.Store(context.Background(), key, []byte("key"))
	n.Store(context.Background(), js, []byte("meta"))
	n.Store(context.Background(), path.Join("acme", "example.com", "sites2", "example.de"), []byte{})
	n.Store(context.Background(), path.Join("acme", "example.com", "sites2", "example.com"), []byte{})

	testList := func(prefix string, want []string) {
		keys, err := n.List(context.Background(), prefix, false)
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		sort.Strings(keys)
		if !reflect.DeepEqual(keys, want) {
			t.Errorf("List() got = %v, want %v", keys, want)
		}
	}

	want := []string{"acme/example.com/sites2/example.com", "acme/example.com/sites2/example.de"}
	prefix := path.Join("acme", "example.com", "sites2")
	testList(prefix, want)

	want = []string{"acme/example.com/sites", "acme/example.com/sites2"}
	prefix = path.Join("acme", "example.com")
	testList(prefix, want)

	want = []string{"acme/example.com/sites/example.com"}
	prefix = path.Join("acme", "example.com", "sites")
	testList(prefix, want)

	want = owant
	prefix = path.Join("acme", "example.com", "sites", "example.com")
	testList(prefix, want)

	want = owant
	prefix = path.Join("acme", "example.com", "sites", "example.com") + "/"
	testList(prefix, want)
}

func TestNats_Exists(t *testing.T) {
	n := getNatsClient("basic")

	n.Store(context.Background(), "testExists", []byte("exists"))

	got := n.Exists(context.Background(), "testExists")
	if !got {
		t.Errorf("Exists() got = %v, want true", got)
	}

	got = n.Exists(context.Background(), "testKeyNotExists")
	if got {
		t.Errorf("Exists() got = %v, want false", got)
	}
}

func TestNats_LockUnlock(t *testing.T) {
	n := getNatsClient("basic")
	lockKey := path.Join("acme", "example.com", "sites", "example.com")

	err := n.Lock(context.Background(), lockKey)
	if err != nil {
		t.Errorf("Unlock() error = %v", err)
	}

	err = n.Unlock(context.Background(), lockKey)
	if err != nil {
		t.Errorf("Unlock() error = %v", err)
	}
}

func TestNats_MultipleLocks(t *testing.T) {
	lockKey := path.Join("acme", "example.com", "sites", "example.com")

	n1 := getNatsClient("basic")
	n2 := getNatsClient("basic")
	n3 := getNatsClient("basic")

	err := n1.Lock(context.Background(), lockKey)
	if err != nil {
		t.Errorf("Lock() error = %v", err)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		n1.Unlock(context.Background(), lockKey)
	}()

	err = n2.Lock(context.Background(), lockKey)
	if err != nil {
		t.Errorf("Lock() error = %v", err)
	}

	n2.Unlock(context.Background(), lockKey)

	time.Sleep(100 * time.Millisecond)
	err = n3.Lock(context.Background(), lockKey)
	if err != nil {
		t.Errorf("Lock() error = %v", err)
	}

	n3.Unlock(context.Background(), lockKey)

	tracker := int32(0)
	wg := sync.WaitGroup{}
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			//<-time.After(time.Duration(200+mrand.Float64()*(2000-200+1)) * time.Millisecond)
			defer wg.Done()
			n := getNatsClient("basic")
			n.ConnectionName = fmt.Sprintf("nats-%d", i)

			err := n.Lock(context.Background(), lockKey)
			if err != nil {
				t.Errorf("Lock() %s error = %v: %d", n.ConnectionName, err, n.getRev("LOCK."+lockKey))
			}

			v := atomic.AddInt32(&tracker, 1)
			if v != 1 {
				panic("Had a concurrent lock")
			}

			// fmt.Printf("Worker %d has the lock (%v)\n", i, v)

			atomic.AddInt32(&tracker, -1)

			err = n.Unlock(context.Background(), lockKey)
			if err != nil {
				t.Errorf("Unlock() %s error = %v: %d", n.ConnectionName, err, n.getRev("LOCK."+lockKey))
			}
		}(i)
	}
	wg.Wait()
}

func FuzzNormalize(f *testing.F) {
	_, _, _, testcases := getTestData()
	for _, tc := range testcases {
		f.Add(tc) // Use f.Add to provide a seed corpus
	}
	f.Fuzz(func(t *testing.T, orig string) {
		if strings.Contains(orig, replaceChar) {
			return
		}

		norm := normalizeNatsKey(orig)
		denorm := denormalizeNatsKey(norm)
		if orig != denorm {
			t.Errorf("Before: %q, after: %q", orig, denorm)
		}
		if utf8.ValidString(orig) && !utf8.ValidString(norm) {
			t.Errorf("Reverse produced invalid UTF-8 string %q", norm)
		}
	})
}
