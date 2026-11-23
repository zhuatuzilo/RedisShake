package reader

import (
	"RedisShake/internal/utils"
	"reflect"
	"testing"
	"time"
)

func TestParseKSNDebounce(t *testing.T) {
	for _, v := range []string{"", "0s", "200ms"} {
		if _, err := parseKSNDebounce(v); err != nil {
			t.Fatal(err)
		}
	}
	for _, v := range []string{"-1ms", "bad"} {
		if _, err := parseKSNDebounce(v); err == nil {
			t.Fatalf("accepted %q", v)
		}
	}
}

func TestKSNDebounceFlushAndDBIsolation(t *testing.T) {
	input := make(chan dbKey, 4)
	a, b := dbKey{0, "key"}, dbKey{1, "key"}
	input <- a
	input <- a
	input <- b
	close(input)
	got := map[dbKey]int{}
	debounceKSN(input, time.Hour, 1024, func(k dbKey) { got[k]++ })
	if !reflect.DeepEqual(got, map[dbKey]int{a: 1, b: 1}) {
		t.Fatal(got)
	}
}

func TestKSNDebounceDisabledPreservesEvents(t *testing.T) {
	input := make(chan dbKey, 3)
	want := []dbKey{{0, "a"}, {0, "a"}, {0, "b"}}
	for _, k := range want {
		input <- k
	}
	close(input)
	var got []dbKey
	debounceKSN(input, 0, 1024, func(k dbKey) { got = append(got, k) })
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
}

func TestKSNDebounceEmitsWhileKeyRemainsHot(t *testing.T) {
	input := make(chan dbKey)
	output := make(chan dbKey, 100)
	done := make(chan struct{})
	go func() { debounceKSN(input, 10*time.Millisecond, 1024, func(k dbKey) { output <- k }); close(done) }()
	stop := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for {
			select {
			case input <- dbKey{0, "hot"}:
			case <-stop:
				return
			}
		}
	}()
	select {
	case <-output:
	case <-time.After(3 * time.Second):
		t.Error("hot key starved")
	}
	close(stop)
	<-producerDone
	close(input)
	<-done
}

func TestKSNDebounceBackpressureDrains(t *testing.T) {
	queue := utils.NewUniqueQueue[dumpItem](1)
	queue.Put(dumpItem{ping: true})
	input := make(chan dbKey, 3)
	keys := []dbKey{{db: 0, key: "same"}, {db: 1, key: "same"}, {db: 0, key: "last"}}
	for _, key := range keys {
		input <- key
	}
	close(input)
	done := make(chan struct{})
	go func() {
		debounceKSN(input, time.Hour, 1, func(k dbKey) { queue.Put(dumpItem{dbKey: k}) })
		queue.Close()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("debounce completed despite full output queue")
	case <-time.After(20 * time.Millisecond):
	}
	if item, ok := queue.Get(); !ok || !item.ping {
		t.Fatal("missing heartbeat")
	}
	for _, key := range keys {
		if item, ok := queue.Get(); !ok || item.dbKey != key {
			t.Fatalf("got %+v, %v; want %+v", item, ok, key)
		}
	}
	<-done
}
