package application

import (
	"testing"
	"time"
)

type sentFrontendEvent struct {
	event *CustomEvent
	id    uint64
}

func TestFrontendEventQueueWaitsForReadyAndAcknowledgement(t *testing.T) {
	sent := make(chan sentFrontendEvent, 2)
	errors := make(chan error, 1)
	queue := newFrontendEventQueue(func(event *CustomEvent, id uint64) {
		sent <- sentFrontendEvent{event: event, id: id}
	}, func(err error) {
		errors <- err
	})
	defer queue.stop()

	queue.enqueue(&CustomEvent{Name: "first"})
	queue.enqueue(&CustomEvent{Name: "second"})
	assertNoFrontendEvent(t, sent)

	queue.markReady()
	first := <-sent
	if first.event.Name != "first" {
		t.Fatalf("first delivery = %q", first.event.Name)
	}
	assertNoFrontendEvent(t, sent)

	queue.acknowledge(first.id)
	second := <-sent
	if second.event.Name != "second" {
		t.Fatalf("second delivery = %q", second.event.Name)
	}
	queue.acknowledge(second.id)

	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

func TestFrontendEventQueueReloadReleasesInFlightAndWaitsForReady(t *testing.T) {
	sent := make(chan sentFrontendEvent, 2)
	errors := make(chan error, 1)
	queue := newFrontendEventQueue(func(event *CustomEvent, id uint64) {
		sent <- sentFrontendEvent{event: event, id: id}
	}, func(err error) {
		errors <- err
	})
	defer queue.stop()

	queue.markReady()
	queue.enqueue(&CustomEvent{Name: "before-reload"})
	<-sent
	queue.markNotReady()
	queue.enqueue(&CustomEvent{Name: "after-reload"})
	assertNoFrontendEvent(t, sent)

	queue.markReady()
	afterReload := <-sent
	if afterReload.event.Name != "after-reload" {
		t.Fatalf("delivery after reload = %q", afterReload.event.Name)
	}
	queue.acknowledge(afterReload.id)
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

func TestFrontendEventQueueContinuesAfterTimeout(t *testing.T) {
	sent := make(chan sentFrontendEvent, 2)
	errors := make(chan error, 1)
	queue := newFrontendEventQueue(func(event *CustomEvent, id uint64) {
		sent <- sentFrontendEvent{event: event, id: id}
	}, func(err error) {
		errors <- err
	})
	queue.timeout = 20 * time.Millisecond
	defer queue.stop()

	queue.markReady()
	queue.enqueue(&CustomEvent{Name: "first"})
	queue.enqueue(&CustomEvent{Name: "second"})
	<-sent
	<-errors
	second := <-sent
	if second.event.Name != "second" {
		t.Fatalf("delivery after timeout = %q", second.event.Name)
	}
	queue.acknowledge(second.id)
}

func TestFrontendEventQueuesDeliverIndependently(t *testing.T) {
	firstWindow := make(chan sentFrontendEvent, 1)
	secondWindow := make(chan sentFrontendEvent, 1)
	firstQueue := newFrontendEventQueue(func(event *CustomEvent, id uint64) {
		firstWindow <- sentFrontendEvent{event: event, id: id}
	}, func(error) {})
	secondQueue := newFrontendEventQueue(func(event *CustomEvent, id uint64) {
		secondWindow <- sentFrontendEvent{event: event, id: id}
	}, func(error) {})
	defer firstQueue.stop()
	defer secondQueue.stop()
	firstQueue.markReady()
	secondQueue.markReady()

	firstQueue.enqueue(&CustomEvent{Name: "first-window"})
	first := <-firstWindow
	firstQueue.enqueue(&CustomEvent{Name: "blocked-behind-first"})
	secondQueue.enqueue(&CustomEvent{Name: "second-window"})
	second := <-secondWindow
	if second.event.Name != "second-window" {
		t.Fatalf("independent window received %q", second.event.Name)
	}
	secondQueue.acknowledge(second.id)
	assertNoFrontendEvent(t, firstWindow)
	firstQueue.acknowledge(first.id)
	queued := <-firstWindow
	firstQueue.acknowledge(queued.id)
}

func assertNoFrontendEvent(t *testing.T, events <-chan sentFrontendEvent) {
	t.Helper()
	select {
	case event := <-events:
		t.Fatalf("unexpected frontend event %q", event.event.Name)
	case <-time.After(50 * time.Millisecond):
	}
}
