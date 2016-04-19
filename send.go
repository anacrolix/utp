package utp

import (
	"time"

	"github.com/anacrolix/missinggo"
)

type send struct {
	acked       bool // Closed with Conn lock.
	payloadSize uint32
	started     missinggo.MonotonicTime
	// This send was skipped in a selective ack.
	resend func()
	conn   *Conn

	acksSkipped int
	resendTimer *time.Timer
	numResends  int
}

// first is true if this is the first time the send is acked. latency is
// calculated for the first ack.
func (s *send) Ack() (latency time.Duration, first bool) {
	first = !s.acked
	if first {
		latency = missinggo.MonotonicSince(s.started)
	}
	s.acked = true
	s.conn.event.Broadcast()
	s.resend = nil
	if s.resendTimer != nil {
		s.resendTimer.Stop()
		s.resendTimer = nil
	}
	return
}

func (s *send) timedOut() {
	s.conn.destroy(errAckTimeout)
}

func (s *send) timeoutResend() {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if missinggo.MonotonicSince(s.started) >= writeTimeout {
		s.timedOut()
		return
	}
	if s.acked || s.conn.closed {
		return
	}
	rt := s.conn.resendTimeout()
	go s.resend()
	s.numResends++
	s.resendTimer.Reset(rt * time.Duration(s.numResends))
}
