package utp

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/anacrolix/missinggo"
)

// Conn is a uTP stream and implements net.Conn. It owned by a Socket, which
// handles dispatching packets to and from Conns.
type Conn struct {
	recv_id, send_id uint16
	seq_nr, ack_nr   uint16
	lastAck          uint16
	lastTimeDiff     uint32
	peerWndSize      uint32
	cur_window       uint32
	connKey          connKey

	// Data waiting to be Read.
	readBuf []byte

	socket     *Socket
	remoteAddr net.Addr
	// The uTP timestamp.
	startTimestamp uint32
	// When the conn was allocated.
	created time.Time

	sentSyn   bool
	synAcked  bool
	gotFin    bool
	wroteFin  bool
	finAcked  bool
	err       error
	closed    bool
	destroyed bool

	unackedSends []*send
	// Inbound payloads, the first is ack_nr+1.
	inbound    []recv
	inboundWnd uint32
	packetsIn  chan packet
	connDeadlines
	latencies        []time.Duration
	pendingSendState bool
}

var (
	_ net.Conn = &Conn{}
)

func (c *Conn) age() time.Duration {
	return time.Since(c.created)
}

func (c *Conn) timestamp() uint32 {
	return nowTimestamp() - c.startTimestamp
}

func (c *Conn) sendPendingState() {
	if !c.pendingSendState {
		return
	}
	if c.destroyed {
		c.sendReset()
	} else {
		c.sendState()
	}
}

func (c *Conn) wndSize() uint32 {
	if len(c.inbound) > maxUnackedInbound/2 {
		return 0
	}
	buffered := uint32(len(c.readBuf)) + c.inboundWnd
	if buffered > recvWindow {
		return 0
	}
	return recvWindow - buffered
}

// Send the given payload with an up to date header.
func (c *Conn) send(_type st, connID uint16, payload []byte, seqNr uint16) (err error) {
	// Always selectively ack the first 64 packets. Don't bother with rest for
	// now.
	selAck := selectiveAckBitmask(make([]byte, 8))
	for i := 1; i < 65; i++ {
		if len(c.inbound) <= i {
			break
		}
		if c.inbound[i].seen {
			selAck.SetBit(i - 1)
		}
	}
	h := header{
		Type:          _type,
		Version:       1,
		ConnID:        connID,
		SeqNr:         seqNr,
		AckNr:         c.ack_nr,
		WndSize:       c.wndSize(),
		Timestamp:     c.timestamp(),
		TimestampDiff: c.lastTimeDiff,
		// Currently always send an 8 byte selective ack.
		Extensions: []extensionField{{
			Type:  extensionTypeSelectiveAck,
			Bytes: selAck,
		}},
	}
	p := h.Marshal()
	// Extension headers are currently fixed in size.
	if len(p) != maxHeaderSize {
		panic("header has unexpected size")
	}
	p = append(p, payload...)
	if logLevel >= 1 {
		log.Printf("writing utp msg to %s: %s", c.remoteAddr, packetDebugString(&h, payload))
	}
	n1, err := c.socket.writeTo(p, c.remoteAddr)
	if err != nil {
		return
	}
	if n1 != len(p) {
		panic(n1)
	}
	c.unpendSendState()
	return
}

func (me *Conn) unpendSendState() {
	me.pendingSendState = false
}

func (c *Conn) pendSendState() {
	c.pendingSendState = true
}

func (me *Conn) writeSyn() {
	if me.sentSyn {
		panic("already sent syn")
	}
	me.write(stSyn, me.recv_id, nil, me.seq_nr)
	return
}

func (c *Conn) write(_type st, connID uint16, payload []byte, seqNr uint16) (n int) {
	switch _type {
	case stSyn, stFin, stData:
	default:
		panic(_type)
	}
	if c.wroteFin {
		panic("can't write after fin")
	}
	if len(payload) > maxPayloadSize {
		payload = payload[:maxPayloadSize]
	}
	err := c.send(_type, connID, payload, seqNr)
	if err != nil {
		c.destroy(fmt.Errorf("error sending packet: %s", err))
		return
	}
	n = len(payload)
	// Copy payload so caller to write can continue to use the buffer.
	if payload != nil {
		payload = append(sendBufferPool.Get().([]byte)[:0:minMTU], payload...)
	}
	send := &send{
		payloadSize: uint32(len(payload)),
		started:     missinggo.MonotonicNow(),
		_type:       _type,
		connID:      connID,
		payload:     payload,
		seqNr:       seqNr,
		conn:        c,
	}
	send.resendTimer = time.AfterFunc(c.resendTimeout(), send.timeoutResend)
	c.unackedSends = append(c.unackedSends, send)
	c.cur_window += send.payloadSize
	c.seq_nr++
	return
}

func (c *Conn) latency() (ret time.Duration) {
	if len(c.latencies) == 0 {
		return initialLatency
	}
	for _, l := range c.latencies {
		ret += l
	}
	ret = (ret + time.Duration(len(c.latencies)) - 1) / time.Duration(len(c.latencies))
	return
}

func (c *Conn) numUnackedSends() (num int) {
	for _, s := range c.unackedSends {
		if !s.acked {
			num++
		}
	}
	return
}

func (c *Conn) sendState() {
	c.send(stState, c.send_id, nil, c.seq_nr)
	sentStatePackets.Add(1)
}

func (c *Conn) sendReset() {
	c.send(stReset, c.send_id, nil, c.seq_nr)
}

// Ack our send with the given sequence number.
func (c *Conn) ack(nr uint16) {
	if !seqLess(c.lastAck, nr) {
		// Already acked.
		return
	}
	i := nr - c.lastAck - 1
	if int(i) >= len(c.unackedSends) {
		log.Printf("got ack ahead of syn (%x > %x)", nr, c.seq_nr-1)
		return
	}
	s := c.unackedSends[i]
	latency, first := s.Ack()
	if first {
		c.cur_window -= s.payloadSize
		c.latencies = append(c.latencies, latency)
		if len(c.latencies) > 10 {
			c.latencies = c.latencies[len(c.latencies)-10:]
		}
	}
	// Trim sends that aren't needed anymore.
	for len(c.unackedSends) != 0 {
		if !c.unackedSends[0].acked {
			// Can't trim unacked sends any further.
			return
		}
		// Trim the front of the unacked sends.
		c.unackedSends = c.unackedSends[1:]
		c.lastAck++
	}
	cond.Broadcast()
}

func (c *Conn) ackTo(nr uint16) {
	if !seqLess(nr, c.seq_nr) {
		return
	}
	for seqLess(c.lastAck, nr) {
		c.ack(c.lastAck + 1)
	}
}

// Return the send state for the sequence number. Returns nil if there's no
// outstanding send for that sequence number.
func (c *Conn) seqSend(seqNr uint16) *send {
	if !seqLess(c.lastAck, seqNr) {
		// Presumably already acked.
		return nil
	}
	i := int(seqNr - c.lastAck - 1)
	if i >= len(c.unackedSends) {
		// No such send.
		return nil
	}
	return c.unackedSends[i]
}

func (c *Conn) resendTimeout() time.Duration {
	l := c.latency()
	ret := missinggo.JitterDuration(3*l, l)
	return ret
}

func (c *Conn) ackSkipped(seqNr uint16) {
	send := c.seqSend(seqNr)
	if send == nil {
		return
	}
	send.acksSkipped++
	if send.acked {
		return
	}
	switch send.acksSkipped {
	case 3, 60:
		ackSkippedResends.Add(1)
		go send.resend()
		send.resendTimer.Reset(c.resendTimeout() * time.Duration(send.numResends))
	default:
	}
}

func (c *Conn) deliver(h header, payload []byte) {
	c.packetsIn <- packet{h, payload}
}

func (c *Conn) deliveryProcessor() {
	for {
		select {
		case p, ok := <-c.packetsIn:
			if !ok {
				return
			}
			c.processDelivery(p.h, p.payload)
			// Process a batch if they arrive in quick succession without
			// acking them until there's a pause.
			timeout := time.After(500 * time.Microsecond)
		batched:
			for {
				select {
				case p, ok := <-c.packetsIn:
					if !ok {
						break batched
					}
					c.processDelivery(p.h, p.payload)
				case <-timeout:
					break batched
				}
			}
			mu.Lock()
			c.sendPendingState()
			mu.Unlock()
		case <-time.After(packetReadTimeout):
			mu.Lock()
			c.destroy(errors.New("no packet read timeout"))
			mu.Unlock()
		}
	}
}

func (c *Conn) lazyDestroy() {
	if c.wroteFin && len(c.unackedSends) <= 1 && (c.gotFin || c.closed) {
		c.destroy(errors.New("lazily destroyed"))
	}
}

func (c *Conn) processDelivery(h header, payload []byte) {
	deliveriesProcessed.Add(1)
	mu.Lock()
	defer mu.Unlock()
	defer c.lazyDestroy()
	defer cond.Broadcast()
	c.assertHeader(h)
	c.peerWndSize = h.WndSize
	c.applyAcks(h)
	if h.Timestamp == 0 {
		c.lastTimeDiff = 0
	} else {
		c.lastTimeDiff = c.timestamp() - h.Timestamp
	}

	if h.Type == stReset {
		c.destroy(errors.New("peer reset"))
		return
	}
	if !c.synAcked {
		if h.Type != stState {
			return
		}
		c.synAcked = true
		c.ack_nr = h.SeqNr - 1
		return
	}
	if h.Type == stState {
		return
	}
	c.pendSendState()
	if !seqLess(c.ack_nr, h.SeqNr) {
		// Already received this packet.
		return
	}
	inboundIndex := int(h.SeqNr - c.ack_nr - 1)
	if inboundIndex < len(c.inbound) && c.inbound[inboundIndex].seen {
		// Already received this packet.
		return
	}
	// Derived from running in production:
	// grep -oP '(?<=packet out of order, index=)\d+' log | sort -n | uniq -c
	// 64 should correspond to 8 bytes of selective ack.
	if inboundIndex >= maxUnackedInbound {
		// Discard packet too far ahead.
		if logLevel >= 1 {
			log.Printf("received packet from %s %d ahead of next seqnr (%x > %x)", c.remoteAddr, inboundIndex, h.SeqNr, c.ack_nr+1)
		}
		return
	}
	// Extend inbound so the new packet has a place.
	for inboundIndex >= len(c.inbound) {
		c.inbound = append(c.inbound, recv{})
	}
	c.inbound[inboundIndex] = recv{true, payload, h.Type}
	c.inboundWnd += uint32(len(payload))
	c.processInbound()
}

func (c *Conn) applyAcks(h header) {
	c.ackTo(h.AckNr)
	for _, ext := range h.Extensions {
		switch ext.Type {
		case extensionTypeSelectiveAck:
			c.ackSkipped(h.AckNr + 1)
			bitmask := selectiveAckBitmask(ext.Bytes)
			for i := 0; i < bitmask.NumBits(); i++ {
				if bitmask.BitIsSet(i) {
					nr := h.AckNr + 2 + uint16(i)
					// log.Printf("selectively acked %d", nr)
					c.ack(nr)
				} else {
					c.ackSkipped(h.AckNr + 2 + uint16(i))
				}
			}
		}
	}
}

func (c *Conn) assertHeader(h header) {
	if h.Type == stSyn {
		if h.ConnID != c.send_id {
			panic(fmt.Sprintf("%d != %d", h.ConnID, c.send_id))
		}
	} else {
		if h.ConnID != c.recv_id {
			panic("erroneous delivery")
		}
	}
}

func (c *Conn) processInbound() {
	// Consume consecutive next packets.
	for !c.gotFin && len(c.inbound) > 0 && c.inbound[0].seen {
		c.ack_nr++
		p := c.inbound[0]
		c.inbound = c.inbound[1:]
		c.inboundWnd -= uint32(len(p.data))
		c.readBuf = append(c.readBuf, p.data...)
		if p.Type == stFin {
			c.gotFin = true
		}
	}
}

func (c *Conn) waitAck(seq uint16) {
	send := c.seqSend(seq)
	if send == nil {
		return
	}
	for !(send.acked || c.destroyed) {
		cond.Wait()
	}
	return
}

func (c *Conn) connect() (err error) {
	mu.Lock()
	defer mu.Unlock()
	c.seq_nr = 1
	c.writeSyn()
	c.sentSyn = true
	if logLevel >= 2 {
		log.Printf("sent syn")
	}
	// c.seq_nr++
	c.waitAck(1)
	if c.err != nil {
		err = c.err
	}
	c.synAcked = true
	cond.Broadcast()
	return err
}

func (c *Conn) writeFin() {
	if c.wroteFin {
		return
	}
	c.write(stFin, c.send_id, nil, c.seq_nr)
	c.wroteFin = true
	cond.Broadcast()
	return
}

func (c *Conn) destroy(reason error) {
	c.destroyed = true
	cond.Broadcast()
	if c.err == nil {
		c.err = reason
	}
	c.detach()
}

func (c *Conn) Close() (err error) {
	mu.Lock()
	defer mu.Unlock()
	c.closed = true
	cond.Broadcast()
	c.writeFin()
	c.lazyDestroy()
	return
}

func (c *Conn) LocalAddr() net.Addr {
	return c.socket.Addr()
}

func (c *Conn) Read(b []byte) (n int, err error) {
	mu.Lock()
	defer mu.Unlock()
	for {
		if len(c.readBuf) != 0 {
			n = copy(b, c.readBuf)
			c.readBuf = c.readBuf[n:]
			return
		}
		if c.gotFin || c.closed {
			err = io.EOF
			return
		}
		if c.destroyed {
			if c.err == nil {
				panic("closed without receiving fin, and no error")
			}
			err = c.err
			return
		}
		if c.connDeadlines.read.passed {
			err = errTimeout
			return
		}
		cond.Wait()
	}
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *Conn) String() string {
	return fmt.Sprintf("<UTPConn %s-%s (%d)>", c.LocalAddr(), c.RemoteAddr(), c.recv_id)
}

func (c *Conn) Write(p []byte) (n int, err error) {
	mu.Lock()
	defer mu.Unlock()
	for len(p) != 0 {
		for {
			if c.wroteFin || c.closed {
				err = errClosed
				return
			}
			if c.destroyed {
				err = c.err
				return
			}
			if c.connDeadlines.write.passed {
				err = errTimeout
				return
			}
			// If peerWndSize is 0, we still want to send something, so don't
			// block until we exceed it.
			if c.synAcked &&
				len(c.unackedSends) < maxUnackedSends &&
				c.cur_window <= c.peerWndSize {
				break
			}
			cond.Wait()
		}
		var n1 int
		n1 = c.write(stData, c.send_id, p, c.seq_nr)
		// c.seq_nr++
		n += n1
		p = p[n1:]
	}
	return
}

func (c *Conn) detach() {
	s := c.socket
	_, ok := s.conns[c.connKey]
	if !ok {
		return
	}
	delete(s.conns, c.connKey)
	close(c.packetsIn)
	s.lazyDestroy()
}
