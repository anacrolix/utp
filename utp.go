package utp

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

type extensionField struct {
	Type  byte
	Bytes []byte
}

type header struct {
	Type          int
	Version       int
	ConnID        uint16
	Timestamp     uint32
	TimestampDiff uint32
	WndSize       uint32
	SeqNr         uint16
	AckNr         uint16
	Extensions    []extensionField
}

func unmarshalExtensions(_type byte, b []byte) (n int, ef []extensionField) {
	for _type != 0 {
		ef = append(ef, extensionField{
			Type:  _type,
			Bytes: append([]byte{}, b[2:b[1]+2]...),
		})
		_type = b[0]
		n += 2 + int(b[1])
	}
	return
}

func (h *header) Unmarshal(b []byte) (n int, err error) {
	// TODO: Are these endian-safe?
	h.Type = int(b[0] >> 4)
	h.Version = int(b[0] & 0xf)
	n, h.Extensions = unmarshalExtensions(b[1], b[20:])
	h.ConnID = binary.BigEndian.Uint16(b[2:4])
	h.Timestamp = binary.BigEndian.Uint32(b[4:8])
	h.TimestampDiff = binary.BigEndian.Uint32(b[8:12])
	h.WndSize = binary.BigEndian.Uint32(b[12:16])
	h.SeqNr = binary.BigEndian.Uint16(b[16:18])
	h.AckNr = binary.BigEndian.Uint16(b[18:20])
	n += 20
	return
}

func (h *header) Marshal() (p []byte) {
	if len(h.Extensions) != 0 {
		panic("marshalling of extensions not implemented")
	}
	p = make([]byte, 20)
	p[0] = byte(h.Type<<4 | 1)
	binary.BigEndian.PutUint16(p[2:4], h.ConnID)
	binary.BigEndian.PutUint32(p[4:8], h.Timestamp)
	binary.BigEndian.PutUint32(p[8:12], h.TimestampDiff)
	binary.BigEndian.PutUint32(p[12:16], h.WndSize)
	binary.BigEndian.PutUint16(p[16:18], h.SeqNr)
	binary.BigEndian.PutUint16(p[18:20], h.AckNr)
	return
}

var (
	_ net.Listener   = &Socket{}
	_ net.PacketConn = &Socket{}
)

const (
	csInvalid = iota
	csSynSent
	csConnected
	csGotFin
	csSentFin
	csDestroy
)

const (
	ST_DATA = iota
	ST_FIN
	ST_STATE
	ST_RESET
	ST_SYN
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
	return c.cs == csConnected
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

func packetDebugString(h *header, payload []byte) string {
	return fmt.Sprintf("%#v: %q", h, payload)
}

func (s *Socket) recvLoop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		s.mu.Unlock()
		b := make([]byte, 2000)
		n, addr, err := s.pc.ReadFrom(b)
		s.mu.Lock()
		if err != nil {
			panic(err)
		}
		if n < 20 {
			continue
		}
		var h header
		hEnd, _ := h.Unmarshal(b[:n])
		log.Printf("recvd utp msg: %s", packetDebugString(&h, b[hEnd:n]))
		if c, ok := s.conns[h.ConnID]; ok {
			c.deliver(h, b[hEnd:n])
			continue
		}
		if h.Type == ST_SYN {
			log.Printf("adding SYN to backlog")
			s.backlog = append(s.backlog, syn{
				seq_nr:  h.SeqNr,
				conn_id: h.ConnID,
				addr:    addr,
			})
			s.event.Broadcast()
			continue
		}
		log.Printf("unhandled message from %s: %q", addr, b[:n])
	}
}

func Dial(addr string) (c *Conn, err error) {
	return DialTimeout(addr, 0)
}

func DialTimeout(addr string, timeout time.Duration) (c *Conn, err error) {
	s, err := NewSocket(":0")
	if err != nil {
		return
	}
	c, err = s.DialTimeout(addr, timeout)
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

func (s *Socket) Dial(addr string) (*Conn, error) {
	return s.DialTimeout(addr, 0)
}

func (s *Socket) DialTimeout(addr string, timeout time.Duration) (c *Conn, err error) {
	netAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return
	}
	c = s.newConn(netAddr)
	c.recv_id = s.newConnID()
	c.send_id = c.recv_id + 1
	s.registerConn(c.recv_id, c)
	connErr := make(chan error, 1)
	go func() {
		connErr <- c.connect()
	}()
	var timeoutCh <-chan time.Time
	if timeout != 0 {
		timeoutCh = time.After(timeout)
	}
	select {
	case err = <-connErr:
	case <-timeoutCh:
		c.Close()
		err = errors.New("dial timeout")
	}
	return
}

func (c *Conn) send(_type int, connID uint16, payload []byte, seqNr uint16) (n int, err error) {
	h := header{
		Type:      _type,
		Version:   1,
		ConnID:    connID,
		SeqNr:     seqNr,
		AckNr:     c.ack_nr,
		WndSize:   15610,
		Timestamp: uint32(time.Now().UnixNano() / 1000),
	}
	p := h.Marshal()
	if len(payload) > 576-len(p) {
		payload = payload[:576-len(p)]
	}
	p = append(p, payload...)
	log.Printf("writing utp msg: %s", packetDebugString(&h, payload))
	n1, err := c.socket.WriteTo(p, c.remoteAddr)
	if err != nil {
		return
	}
	if n1 != len(p) {
		panic(n1)
	}
	n = len(payload)
	if _type != ST_STATE {
		time.AfterFunc(1*time.Second, c.resendFunc(c.seq_nr, p))
		time.AfterFunc(2*time.Second, c.resendFunc(c.seq_nr, p))
		time.AfterFunc(3*time.Second, c.timeoutFunc(c.seq_nr))
	}
	return
}

func (c *Conn) resendFunc(seqNr uint16, packet []byte) func() {
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !seqLess(c.lastAck, seqNr) {
			return
		}
		n, err := c.socket.WriteTo(packet, c.remoteAddr)
		if err != nil || n != len(packet) {
			c.destroy()
		}
	}
}

func (c *Conn) timeoutFunc(seqNr uint16) func() {
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !seqLess(c.lastAck, seqNr) {
			return
		}
		// log.Printf(, ...)
		c.destroy()
	}
}

func (c *Conn) sendState() {
	c.send(ST_STATE, c.send_id, nil, c.seq_nr)
}

func seqLess(a, b uint16) bool {
	return a < b
}

func (c *Conn) deliver(h header, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.event.Broadcast()
	if h.ConnID != c.recv_id {
		panic("wrong conn id")
	}
	if seqLess(c.lastAck, h.AckNr) {
		c.lastAck = h.AckNr
	}
	if h.Type == ST_STATE {
		if c.cs == csSynSent {
			c.ack_nr = h.SeqNr - 1
			c.cs = csConnected
		}
		return
	}
	switch c.cs {
	case csConnected:
		if h.Type != ST_STATE && h.SeqNr != c.ack_nr+1 {
			log.Printf("out of order packet: expected %x got %x", c.ack_nr+1, h.SeqNr)
			return
		}
		c.ack_nr = h.SeqNr
	case csSentFin:
		if h.AckNr == c.seq_nr-1 {
			c.cs = csDestroy
		}
	}
	if h.Type != ST_STATE {
		c.sendState()
	}
	if h.Type == ST_FIN {
		// Skip csGotFin because we can't be missing any packets with the
		// current design.
		log.Print("set destroy")
		c.cs = csDestroy
	}
	log.Printf("appending to readbuf")
	c.readBuf = append(c.readBuf, payload...)
	// c.event.Broadcast()
}

func (c *Conn) waitAck(seq uint16) {
	for {
		if c.cs == csDestroy {
			return
		}
		if !seqLess(c.lastAck, seq) {
			return
		}
		c.event.Wait()
	}
}

func (c *Conn) connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq_nr = 1
	_, err := c.send(ST_SYN, c.recv_id, nil, c.seq_nr)
	if err != nil {
		return err
	}
	c.cs = csSynSent
	log.Printf("sent syn")
	c.seq_nr++
	c.waitAck(1)
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
	_c.sendState()
	// _c.seq_nr++
	_c.cs = csConnected
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

func (c *Conn) finish() {
	if c.cs == csSentFin {
		return
	}
	c.send(ST_FIN, c.send_id, nil, c.seq_nr)
	c.seq_nr++ // Spec says set to "eof_pkt".
	c.cs = csSentFin
}

func (c *Conn) destroy() {
	c.cs = csDestroy
	c.event.Broadcast()
}

func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.finish()
	return nil
}

func (c *Conn) LocalAddr() net.Addr {
	return c.socket.LocalAddr()
}

func (c *Conn) Read(b []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if len(c.readBuf) != 0 {
			break
		}
		if c.cs == csDestroy {
			err = io.EOF
			return
		}
		log.Printf("nothing to read, state=%d", c.cs)
		c.event.Wait()
	}
	// log.Printf("read some data!")
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

func (c *Conn) String() string {
	return fmt.Sprintf("<UTPConn %s-%s>", c.LocalAddr(), c.RemoteAddr())
}

func (c *Conn) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(p) != 0 {
		if c.cs != csConnected {
			err = io.ErrClosedPipe
			return
		}
		var n1 int
		n1, err = c.send(ST_DATA, c.send_id, p, c.seq_nr)
		if err != nil {
			return
		}
		c.seq_nr++
		c.waitAck(c.seq_nr - 1)
		n += n1
		p = p[n1:]
	}
	return
}
