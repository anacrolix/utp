package utp

import (
	"log"
	"time"

	"testing"
)

func TestUTPPingPong(t *testing.T) {
	s, err := NewSocket("localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	pingerClosed := make(chan struct{})
	go func() {
		defer close(pingerClosed)
		b, err := Dial(s.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer b.Close()
		n, err := b.Write([]byte("ping"))
		if err != nil {
			t.Fatal(err)
		}
		if n != 4 {
			panic(n)
		}
		buf := make([]byte, 4)
		b.Read(buf)
		if string(buf) != "pong" {
			t.Fatal("expected pong")
		}
		log.Printf("got pong")
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
	log.Print("got ping")
	n, err = a.Write([]byte("pong"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		panic(n)
	}
	log.Print("waiting for pinger to close")
	<-pingerClosed
}

func TestDialTimeout(t *testing.T) {
	s, _ := NewSocket("localhost:0")
	defer s.Close()
	conn, err := DialTimeout(s.Addr().String(), 10*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Fatal("expected timeout")
	}
	t.Log(err)
}
