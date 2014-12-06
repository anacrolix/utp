package utp

import (
	"log"

	"testing"
)

func TestUTPPingPong(t *testing.T) {
	s, err := NewSocket("localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		b, err := Dial(s.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		n, err := b.Write([]byte("ping"))
		if err != nil {
			t.Fatal(err)
		}
		if n != 4 {
			panic(n)
		}
	}()
	a, err := s.Accept()
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("accepted %s", a)
	buf := make([]byte, 42)
	n, err := a.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if string(buf[:n]) != "ping" {
		t.Fatalf("didn't get ping, got %q", buf[:n])
	}
	log.Print("such wow")
}
