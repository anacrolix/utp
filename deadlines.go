package utp

import "time"

type deadlineCallback struct {
	deadline time.Time
	timer    *time.Timer
	callback func()
}

func (me *deadlineCallback) deadlineExceeded() bool {
	return !me.deadline.IsZero() && !time.Now().Before(me.deadline)
}

func (me *deadlineCallback) updateTimer() {
	if me.timer != nil {
		me.timer.Stop()
	}
	if me.deadline.IsZero() {
		return
	}
	if me.callback == nil {
		panic("deadline callback is nil")
	}
	me.timer = time.AfterFunc(me.deadline.Sub(time.Now()), me.callback)
}

func (me *deadlineCallback) setDeadline(t time.Time) {
	me.deadline = t
	me.updateTimer()
}

func (me *deadlineCallback) setCallback(f func()) {
	me.callback = f
	me.updateTimer()
}

type connDeadlines struct {
	// mu          sync.Mutex
	read, write deadlineCallback
}

func (c *connDeadlines) SetDeadline(t time.Time) error {
	c.read.setDeadline(t)
	c.write.setDeadline(t)
	return nil
}

func (c *connDeadlines) SetReadDeadline(t time.Time) error {
	c.read.setDeadline(t)
	return nil
}

func (c *connDeadlines) SetWriteDeadline(t time.Time) error {
	c.write.setDeadline(t)
	return nil
}
