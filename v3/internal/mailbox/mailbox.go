// Package mailbox provides asynchronous FIFO message processing.
package mailbox

import "sync"

// Mailbox processes messages in send order on one persistent worker without
// blocking senders on the consumer. The queue is unbounded and safe for
// concurrent senders.
type Mailbox[T any] struct {
	lock    sync.Mutex
	pending []T
	consume func(T)
	wake    chan struct{}
	done    chan struct{}
	closed  bool
	drain   bool
}

// New returns a mailbox that calls consume for one message at a time. The
// consumer owns panic reporting and recovery so the caller can use its normal
// application-level panic handler.
func New[T any](consume func(T)) *Mailbox[T] {
	result := &Mailbox[T]{
		consume: consume,
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go result.run()
	return result
}

// Send adds message to the mailbox. It returns false after the mailbox has
// been closed or stopped.
func (m *Mailbox[T]) Send(message T) bool {
	m.lock.Lock()
	if m.closed {
		m.lock.Unlock()
		return false
	}
	m.pending = append(m.pending, message)
	m.lock.Unlock()
	m.signal()
	return true
}

// Close stops accepting messages and lets the worker drain its queue.
func (m *Mailbox[T]) Close() {
	m.shutdown(true)
}

// Stop stops accepting messages and discards messages that have not started.
func (m *Mailbox[T]) Stop() {
	m.shutdown(false)
}

// Done is closed after the worker exits.
func (m *Mailbox[T]) Done() <-chan struct{} {
	return m.done
}

func (m *Mailbox[T]) shutdown(drain bool) {
	m.lock.Lock()
	if m.closed {
		if !drain && m.drain {
			m.drain = false
			clear(m.pending)
			m.pending = nil
		}
		m.lock.Unlock()
		m.signal()
		return
	}
	m.closed = true
	m.drain = drain
	if !drain {
		clear(m.pending)
		m.pending = nil
	}
	m.lock.Unlock()
	m.signal()
}

func (m *Mailbox[T]) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Mailbox[T]) run() {
	defer close(m.done)

	for {
		<-m.wake
		for {
			m.lock.Lock()
			if len(m.pending) == 0 {
				m.pending = nil
				closed := m.closed
				m.lock.Unlock()
				if closed {
					return
				}
				break
			}

			message := m.pending[0]
			var zero T
			m.pending[0] = zero
			m.pending = m.pending[1:]
			m.lock.Unlock()

			m.consume(message)

			m.lock.Lock()
			stop := m.closed && !m.drain
			m.lock.Unlock()
			if stop {
				return
			}
		}
	}
}
