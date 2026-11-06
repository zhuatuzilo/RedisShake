package reader

import (
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
	debounceKSN(input, time.Hour, func(k dbKey) { got[k]++ })
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
	debounceKSN(input, 0, func(k dbKey) { got = append(got, k) })
	if !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
}

func TestKSNDebounceEmitsWhileKeyRemainsHot(t *testing.T) {
	input := make(chan dbKey)
	output := make(chan dbKey, 100)
	done := make(chan struct{})
	go func() { debounceKSN(input, 10*time.Millisecond, func(k dbKey) { output <- k }); close(done) }()
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
