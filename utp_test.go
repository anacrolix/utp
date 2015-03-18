package utp

import (
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	_ "github.com/anacrolix/envpprof"

	"github.com/bradfitz/iter"

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
	if stMax != stSyn {
		t.FailNow()
	}
}

func TestUTPRawConn(t *testing.T) {
	l, err := NewSocket("")
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
	utpPeer, err := func() *Socket {
		s, _ := NewSocket("")
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
			n, _, err := l.PacketConn().ReadFrom(b)
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

func TestConnReadDeadline(t *testing.T) {
	ls, _ := NewSocket("localhost:0")
	ds, _ := NewSocket("localhost:0")
	dcReadErr := make(chan error)
	go func() {
		c, _ := ds.Dial(ls.Addr().String())
		defer c.Close()
		_, err := c.Read(nil)
		dcReadErr <- err
	}()
	c, _ := ls.Accept()
	dl := time.Now().Add(time.Millisecond)
	c.SetReadDeadline(dl)
	_, err := c.Read(nil)
	if !err.(net.Error).Timeout() {
		t.FailNow()
	}
	// The deadline has passed.
	if !time.Now().After(dl) {
		t.FailNow()
	}
	// Returns timeout on subsequent read.
	_, err = c.Read(nil)
	if !err.(net.Error).Timeout() {
		t.FailNow()
	}
	// Disable the deadline.
	c.SetReadDeadline(time.Time{})
	readReturned := make(chan struct{})
	go func() {
		c.Read(nil)
		close(readReturned)
	}()
	select {
	case <-readReturned:
		// Read returned but shouldn't have.
		t.FailNow()
	case <-time.After(time.Millisecond):
	}
	c.Close()
	<-readReturned
	if err := <-dcReadErr; err != io.EOF {
		t.Fatalf("dial conn read returned %s", err)
	}
}

func connectSelfLots(n int, t testing.TB) {
	s, err := NewSocket("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for range iter.N(n) {
			c, err := s.Accept()
			if err != nil {
				log.Fatal(err)
			}
			defer c.Close()
		}
	}()
	var conns []net.Conn
	for range iter.N(n) {
		c, err := s.DialTimeout(s.Addr().String(), 15*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
		// log.Printf("%d", len(conns))
	}
	for _, c := range conns {
		c.Close()
	}
	s.mu.Lock()
	for len(s.conns) != 0 {
		// log.Printf("socket conns: %d", len(s.conns))
		s.event.Wait()
	}
	s.mu.Unlock()
	s.Close()
}

// Connect to ourself heaps.
func TestConnectSelf(t *testing.T) {
	// A rough guess says that at worst, I can only have 0x10000/3 connections
	// to the same socket, due to fragmentation in the assigned connection
	// IDs.
	connectSelfLots(func() int {
		if testing.Short() {
			return 0x100
		} else {
			return 0x5000
		}
	}(), t)
}

func BenchmarkConnectSelf(b *testing.B) {
	for range iter.N(b.N) {
		connectSelfLots(2, b)
	}
}

func BenchmarkNewCloseSocket(b *testing.B) {
	for range iter.N(b.N) {
		s, err := NewSocket("localhost:0")
		if err != nil {
			b.Fatal(err)
		}
		err = s.Close()
		if err != nil {
			b.Fatal(err)
		}
	}
}
