package reader

import (
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"RedisShake/internal/client"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/utils"
)

// Set REDIS_SERVER to a real Redis binary. Each test uses private loopback
// ports and disables persistence; no existing Redis instance is touched.
func startKSNRedis(t *testing.T) string {
	t.Helper()
	binary := os.Getenv("REDIS_SERVER")
	if binary == "" {
		t.Skip("REDIS_SERVER is required for real Redis integration")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	logFile, err := os.Create(filepath.Join(t.TempDir(), "redis.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "--bind", "127.0.0.1", "--port", strconv.Itoa(port), "--save", "", "--appendonly", "no", "--notify-keyspace-events", "KEA")
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait(); _ = logFile.Close() })
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, e := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if e == nil {
			c.Close()
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("Redis startup timed out")
	return ""
}

func TestKSNRealRedis(t *testing.T) {
	for _, scan := range []bool{false, true} {
		t.Run("scan="+strconv.FormatBool(scan), func(t *testing.T) { testKSNRealRedis(t, scan) })
	}
}

func testKSNRealRedis(t *testing.T, scan bool) {
	sourceAddr, targetAddr := startKSNRedis(t), startKSNRedis(t)
	connect := func(addr string) *client.Redis {
		return client.NewRedisClient(context.Background(), addr, "", "", false, client.TlsConfig{}, false)
	}
	source, target, writer := connect(sourceAddr), connect(targetAddr), connect(targetAddr)
	defer source.Close()
	defer target.Close()
	defer writer.Close()
	old := config.Opt.Advanced
	config.Opt.Advanced.TargetRedisProtoMaxBulkLen = 1024 * 1024
	config.Opt.Advanced.RDBRestoreCommandBehavior = "rewrite"
	defer func() { config.Opt.Advanced = old }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &scanStandaloneReader{opts: &ScanReaderOptions{KSN: true, Scan: scan, Count: 1, KSNDebounceInterval: "100ms"}, ch: make(chan *entry.Entry, 1024), needDumpQueue: utils.NewUniqueQueue(1024), needRestoreChan: make(chan *needRestoreItem, 1024)}
	r.opts.Address = sourceAddr
	var restores atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		db := 0
		for e := range r.StartRead(ctx)[0] {
			if db != e.DbId {
				writer.Do("SELECT", e.DbId)
				db = e.DbId
			}
			args := make([]interface{}, len(e.Argv))
			for i, a := range e.Argv {
				args[i] = a
			}
			writer.Do(args...)
			if e.Argv[0] == "RESTORE" {
				restores.Add(1)
			}
		}
	}()
	// StartRead returns only after subscription acknowledgement.
	get := func(k string) interface{} { target.Send("GET", k); v, _ := target.Receive(); return v }
	hget := func() interface{} { target.Send("HGET", "hot", "field"); v, _ := target.Receive(); return v }
	deadline := time.Now().Add(5 * time.Second)
	for {
		if source.Do("PUBSUB", "NUMPAT").(int64) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no subscription")
		}
		time.Sleep(time.Millisecond)
	}
	await := func(label string, check func() bool) {
		t.Helper()
		until := time.Now().Add(5 * time.Second)
		for time.Now().Before(until) {
			if check() {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Fatalf("%s did not converge", label)
	}
	for i := 0; i < 100; i++ {
		source.Do("HSET", "hot", "field", strconv.Itoa(i))
	}
	await("hot hash", func() bool { return hget() == "99" })
	t.Logf("100 HSET notifications produced %d RESTOREs before convergence", restores.Load())
	if n := restores.Load(); n >= 100 {
		t.Fatalf("notifications were not coalesced: %d", n)
	}
	source.Do("DEL", "hot")
	await("delete", func() bool { return target.Do("EXISTS", "hot").(int64) == 0 })
	for i := 0; i < 30; i++ {
		source.Do("SET", "recreate", "old")
		source.Do("DEL", "recreate")
		source.Do("SET", "recreate", "new")
	}
	await("recreate", func() bool { return get("recreate") == "new" })
	source.Do("SET", "expiry", "value")
	await("pre-expiry", func() bool { return get("expiry") == "value" })
	source.Do("PEXPIRE", "expiry", 30)
	await("expiry", func() bool { return target.Do("EXISTS", "expiry").(int64) == 0 })
	source.Do("SELECT", 1)
	source.Do("SET", "recreate", "db1")
	target.Do("SELECT", 1)
	await("DB isolation", func() bool { return get("recreate") == "db1" })
	target.Do("SELECT", 0)
	if get("recreate") != "new" {
		t.Fatal("DB0 overwritten")
	}
	// Ensure subscriber accepted the update, then cancel before the next window.
	source.Do("SELECT", 0)
	source.Do("SET", "shutdown", "flushed")
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown did not drain")
	}
	if get("shutdown") != "flushed" {
		t.Fatal("pending shutdown update lost")
	}
	t.Logf("real Redis validation passed; initial 100 HSETs coalesced; total RESTOREs=%d", restores.Load())
}
