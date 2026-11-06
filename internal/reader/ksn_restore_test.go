package reader

import (
	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"bufio"
	"context"
	"io"
	"net"
	"reflect"
	"testing"
	"time"
)

// Simulate an old in-flight DUMP, deletion/expiry, and recreation. Only
// restore owns output, so an old RESTORE cannot overtake the later DEL.
func TestKSNRestoreDeletionOrdering(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	serverDone := make(chan error, 1)
	go func() {
		c, e := listener.Accept()
		if e != nil {
			serverDone <- e
			return
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
		_, e = proto.NewReader(bufio.NewReader(c)).ReadReply()
		if e == nil {
			_, e = io.WriteString(c, "+PONG\r\n$3\r\nold\r\n:-1\r\n$-1\r\n:-2\r\n$3\r\nold\r\n:-2\r\n$3\r\nnew\r\n:-1\r\n")
		}
		serverDone <- e
	}()
	c := client.NewRedisClient(context.Background(), listener.Addr().String(), "", "", false, client.TlsConfig{}, false)
	defer c.Close()
	oldLimit := config.Opt.Advanced.TargetRedisProtoMaxBulkLen
	config.Opt.Advanced.TargetRedisProtoMaxBulkLen = 1024
	defer func() { config.Opt.Advanced.TargetRedisProtoMaxBulkLen = oldLimit }()
	r := &scanStandaloneReader{opts: &ScanReaderOptions{}, dumpClient: c, ch: make(chan *entry.Entry, 8), needRestoreChan: make(chan *needRestoreItem, 4)}
	for i := 0; i < 4; i++ {
		r.needRestoreChan <- &needRestoreItem{key: "key"}
	}
	close(r.needRestoreChan)
	r.restore()
	var got []string
	for e := range r.ch {
		got = append(got, e.Argv[0])
	}
	if !reflect.DeepEqual(got, []string{"RESTORE", "DEL", "DEL", "RESTORE"}) {
		t.Fatal(got)
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}
