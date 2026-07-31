package application

import (
	"fmt"
	"sync"
	"time"

	"github.com/wailsapp/wails/v3/internal/mailbox"
)

const frontendEventAckTimeout = 5 * time.Second

type frontendEventQueue struct {
	events  *mailbox.Mailbox[*CustomEvent]
	send    func(*CustomEvent, uint64)
	onError func(error)
	timeout time.Duration

	lock        sync.Mutex
	closed      bool
	ready       bool
	readySignal chan struct{}
	nextID      uint64
	inFlightID  uint64
	inFlight    chan struct{}
	stopped     chan struct{}
	stopOnce    sync.Once
}

func newFrontendEventQueue(send func(*CustomEvent, uint64), onError func(error)) *frontendEventQueue {
	result := &frontendEventQueue{
		send:        send,
		onError:     onError,
		timeout:     frontendEventAckTimeout,
		readySignal: make(chan struct{}),
		stopped:     make(chan struct{}),
	}
	result.events = mailbox.New(func(event *CustomEvent) {
		defer handlePanic()
		result.deliver(event)
	})
	return result
}

func (q *frontendEventQueue) enqueue(event *CustomEvent) {
	q.events.Send(event)
}

func (q *frontendEventQueue) markReady() {
	q.lock.Lock()
	if !q.ready {
		q.ready = true
		close(q.readySignal)
	}
	q.lock.Unlock()
}

func (q *frontendEventQueue) markNotReady() {
	q.lock.Lock()
	if q.ready {
		q.ready = false
		q.readySignal = make(chan struct{})
	}
	inFlight := q.inFlight
	q.inFlight = nil
	q.inFlightID = 0
	q.lock.Unlock()

	if inFlight != nil {
		select {
		case inFlight <- struct{}{}:
		default:
		}
	}
}

func (q *frontendEventQueue) acknowledge(id uint64) {
	q.lock.Lock()
	if q.inFlightID != id {
		q.lock.Unlock()
		return
	}
	inFlight := q.inFlight
	q.lock.Unlock()

	select {
	case inFlight <- struct{}{}:
	default:
	}
}

func (q *frontendEventQueue) stop() {
	q.stopOnce.Do(func() {
		q.lock.Lock()
		q.closed = true
		q.lock.Unlock()
		close(q.stopped)
		q.events.Stop()
	})
}

func (q *frontendEventQueue) deliver(event *CustomEvent) {
	var id uint64
	var acknowledged chan struct{}

	for {
		q.lock.Lock()
		if q.closed {
			q.lock.Unlock()
			return
		}
		if q.ready {
			q.nextID++
			id = q.nextID
			acknowledged = make(chan struct{}, 1)
			q.inFlightID = id
			q.inFlight = acknowledged
			q.lock.Unlock()
			break
		}
		ready := q.readySignal
		q.lock.Unlock()

		select {
		case <-ready:
		case <-q.stopped:
			return
		}
	}

	q.send(event, id)
	timer := time.NewTimer(q.timeout)
	select {
	case <-acknowledged:
	case <-timer.C:
		q.onError(fmt.Errorf("frontend event %q delivery acknowledgement %d timed out", event.Name, id))
	case <-q.stopped:
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	q.lock.Lock()
	if q.inFlightID == id {
		q.inFlightID = 0
		q.inFlight = nil
	}
	q.lock.Unlock()
}
