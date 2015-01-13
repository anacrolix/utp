package utp

import (
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	utp "."
	"bitbucket.org/anacrolix/go.torrent/util"
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

func TestMinMaxHeaderType(t *testing.T) {
	if ST_MAX != ST_SYN {
		t.FailNow()
	}
}

func TestUTPRawConn(t *testing.T) {
	l, err := utp.NewSocket("")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	go func() {
		for {
			_, err := l.Accept()
			if err != nil {
				break
			}
		}
	}()
	// Connect a UTP peer to see if the RawConn will still work.
	log.Print("dialing")
	utpPeer, err := func() *utp.Socket {
		s, _ := utp.NewSocket("")
		return s
	}().Dial(fmt.Sprintf("localhost:%d", util.AddrPort(l.Addr())))
	log.Print("dial returned")
	if err != nil {
		t.Fatalf("error dialing utp listener: %s", err)
	}
	defer utpPeer.Close()
	peer, err := net.ListenPacket("udp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	msgsReceived := 0
	const N = 5000 // How many messages to send.
	readerStopped := make(chan struct{})
	// The reader goroutine.
	go func() {
		defer close(readerStopped)
		b := make([]byte, 500)
		for i := 0; i < N; i++ {
			n, _, err := l.ReadFrom(b)
			if err != nil {
				t.Fatalf("error reading from raw conn: %s", err)
			}
			msgsReceived++
			var d int
			fmt.Sscan(string(b[:n]), &d)
			if d != i {
				log.Printf("got wrong number: expected %d, got %d", i, d)
			}
		}
	}()
	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("localhost:%d", util.AddrPort(l.Addr())))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < N; i++ {
		_, err := peer.WriteTo([]byte(fmt.Sprintf("%d", i)), udpAddr)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Microsecond)
	}
	select {
	case <-readerStopped:
	case <-time.After(time.Second):
		t.Fatal("reader timed out")
	}
	if msgsReceived != N {
		t.Fatalf("messages received: %d", msgsReceived)
	}
}
