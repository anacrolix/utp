package utp

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

type Socket struct {
	mu      sync.Mutex
	event   sync.Cond
	pc      net.PacketConn
	conns   map[uint16]*Conn
	backlog []syn
}

type syn struct {
	seq_nr, conn_id uint16
	addr            net.Addr
}

type header struct {
	Type    int
	Version int
	ConnID  uint16
	SeqNr   uint16
	AckNr   uint16
}

var (
	_ net.Listener   = &Socket{}
	_ net.PacketConn = &Socket{}
)

const (
	CS_INVALID = iota
	CS_SYN_SENT
	CS_CONNECTED
)

type Conn struct {
	mu    sync.Mutex
	event sync.Cond

	recv_id, send_id uint16
	seq_nr, ack_nr   uint16
	lastAck          uint16

	readBuf []byte

	socket     net.PacketConn
	remoteAddr net.Addr

	cs int
}

var (
	_ net.Conn = &Conn{}
)

func (c *Conn) connected() bool {
	return c.cs == CS_CONNECTED
}

func NewSocket(addr string) (s *Socket, err error) {
	s = &Socket{}
	s.event.L = &s.mu
	s.pc, err = net.ListenPacket("udp", addr)
	if err != nil {
		return
	}
	go s.recvLoop()
	return
}

func (s *Socket) recvLoop() {
	for {
		b := make([]byte, 1000)
		n, addr, err := s.pc.ReadFrom(b)
		if err != nil {
			panic(err)
		}
		log.Printf("received from %s: %q", addr, b[:n])
		if n < 20 {
			continue
		}
		var h header
		h.ConnID = binary.BigEndian.Uint16(b[2:4])
		h.SeqNr = binary.BigEndian.Uint16(b[16:18])
		h.AckNr = binary.BigEndian.Uint16(b[18:20])
		h.Type = int(b[0] >> 4)
		if c, ok := s.conns[h.ConnID]; ok {
			c.deliver(h, b[20:n])
			continue
		}
		if h.Type == ST_SYN {
			log.Printf("adding SYN to backlog")
			s.mu.Lock()
			s.backlog = append(s.backlog, syn{
				seq_nr:  h.SeqNr,
				conn_id: h.ConnID,
				addr:    addr,
			})
			s.event.Broadcast()
			s.mu.Unlock()
			continue
		}
		log.Printf("unhandled message from %s: %q", addr, b[:n])
	}
}

func Dial(addr string) (c *Conn, err error) {
	s, err := NewSocket(":0")
	if err != nil {
		return
	}
	c, err = s.Dial(addr)
	return
}

func (s *Socket) newConnID() (id uint16) {
	for {
		id = uint16(rand.Int())
		if _, ok := s.conns[id+1]; !ok {
			return
		}
	}
}

func (s *Socket) newConn(addr net.Addr) (c *Conn) {
	c = &Conn{
		socket:     s.pc,
		remoteAddr: addr,
	}
	c.event.L = &c.mu
	return
}

func (s *Socket) Dial(addr string) (c *Conn, err error) {
	netAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return
	}
	c = s.newConn(netAddr)
	c.recv_id = s.newConnID()
	c.send_id = c.recv_id + 1
	s.registerConn(c.recv_id, c)
	go c.connect()
	return
}

func (c *Conn) send(_type int, connID uint16, payload []byte) (n int, err error) {
	p := make([]byte, 20+len(payload))
	p[0] = byte(_type<<4 | 1)
	binary.BigEndian.PutUint16(p[2:4], connID)
	binary.BigEndian.PutUint32(p[4:8], uint32(time.Now().UnixNano()/1000))
	binary.BigEndian.PutUint16(p[16:18], c.seq_nr)
	binary.BigEndian.PutUint16(p[18:20], c.ack_nr)
	copy(p[20:], payload)
	log.Printf("writing to %s: %q", c.remoteAddr, p)
	n1, err := c.socket.WriteTo(p, c.remoteAddr)
	if err != nil {
		return
	}
	n = n1 - 20
	c.seq_nr++
	return
}

func (c *Conn) deliver(h header, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("delivering message to %s", c)
	if h.ConnID != c.recv_id {
		panic("wrong conn id")
	}
	c.ack_nr = h.SeqNr
	c.send(ST_STATE, c.send_id, nil)
	c.lastAck = h.AckNr
	c.readBuf = append(c.readBuf, payload...)
	c.event.Broadcast()
}

func (c *Conn) waitAck(seq uint16) {
	for c.lastAck < seq {
		c.event.Wait()
	}
}

func (c *Conn) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq_nr = 1
	_, err := c.send(ST_SYN, c.recv_id, nil)
	if err != nil {
		panic(err)
	}
	log.Printf("sent syn")
	c.waitAck(1)
	log.Printf("syn acked")
	c.cs = CS_CONNECTED
	c.event.Broadcast()
	return err
}

func (s *Socket) registerConn(recvID uint16, c *Conn) {
	if s.conns == nil {
		s.conns = make(map[uint16]*Conn)
	}
	if _, ok := s.conns[recvID]; ok {
		panic("multiple conns registered on same ID")
	}
	s.conns[recvID] = c
}

func (s *Socket) Accept() (c net.Conn, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.backlog) == 0 {
		s.event.Wait()
	}
	syn := s.backlog[0]
	s.backlog = s.backlog[1:]
	if _, ok := s.conns[syn.conn_id+1]; ok {
		// SYN for existing connection.
		panic("packet wasn't delivered to conn")
	}
	_c := s.newConn(syn.addr)
	_c.send_id = syn.conn_id
	_c.recv_id = _c.send_id + 1
	_c.seq_nr = uint16(rand.Int())
	_c.ack_nr = syn.seq_nr
	_c.send(ST_STATE, _c.send_id, nil)
	_c.cs = CS_CONNECTED
	s.registerConn(_c.recv_id, _c)
	c = _c
	return
}

func (s *Socket) Addr() net.Addr {
	return s.pc.LocalAddr()
}

func (s *Socket) Close() error {
	s.pc = nil
	return nil
}

func (s *Socket) LocalAddr() net.Addr {
	return s.pc.LocalAddr()
}

func (s *Socket) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, nil
}

func (s *Socket) SetDeadline(time.Time) error {
	return nil
}

func (s *Socket) SetReadDeadline(time.Time) error {
	return nil
}

func (s *Socket) SetWriteDeadline(time.Time) error {
	return nil
}

func (s *Socket) WriteTo([]byte, net.Addr) (int, error) {
	return 0, nil
}

func (c *Conn) Close() error {
	return nil
}

func (c *Conn) LocalAddr() net.Addr {
	return c.socket.LocalAddr()
}

func (c *Conn) Read(b []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.readBuf) == 0 {
		c.event.Wait()
	}
	log.Printf("read some data!")
	n = copy(b, c.readBuf)
	c.readBuf = c.readBuf[n:]
	return
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (s *Conn) SetDeadline(time.Time) error {
	return nil
}

func (s *Conn) SetReadDeadline(time.Time) error {
	return nil
}

func (s *Conn) SetWriteDeadline(time.Time) error {
	return nil
}

const (
	ST_DATA = iota
	ST_FIN
	ST_STATE
	ST_RESET
	ST_SYN
)

func (c *Conn) String() string {
	return fmt.Sprintf("<UTPConn %s-%s>", c.LocalAddr(), c.RemoteAddr())
}

func (c *Conn) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for !c.connected() {
		c.event.Wait()
	}
	for len(p) != 0 {
		var n1 int
		n1, err = c.send(ST_DATA, c.send_id, p)
		if err != nil {
			return
		}
		n += n1
		p = p[n1:]
	}
	return
}
