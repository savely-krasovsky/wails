package application_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wailsapp/wails/v3/pkg/application"

	"github.com/matryer/is"
)

type mockNotifier struct {
	mu     sync.Mutex
	Events []*application.CustomEvent
}

func (m *mockNotifier) dispatchEventToWindows(event *application.CustomEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Events = append(m.Events, event)
}

func (m *mockNotifier) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Events = []*application.CustomEvent{}
}

func Test_EventsOn(t *testing.T) {
	i := is.New(t)
	notifier := &mockNotifier{}
	eventProcessor := application.NewWailsEventProcessor(notifier.dispatchEventToWindows)
	t.Cleanup(eventProcessor.Close)

	// Test OnApplicationEvent
	eventName := "test"
	var counter atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	unregisterFn := eventProcessor.On(eventName, func(event *application.CustomEvent) {
		// This is called in a goroutine
		counter.Add(1)
		wg.Done()
	})
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	wg.Wait()
	i.Equal(1, int(counter.Load()))

	// Unregister
	notifier.Reset()
	unregisterFn()
	counter.Store(0)
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	i.Equal(0, int(counter.Load()))

}

func Test_EventsOnce(t *testing.T) {
	i := is.New(t)
	notifier := &mockNotifier{}
	eventProcessor := application.NewWailsEventProcessor(notifier.dispatchEventToWindows)
	t.Cleanup(eventProcessor.Close)

	// Test OnApplicationEvent
	eventName := "test"
	var counter atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	unregisterFn := eventProcessor.Once(eventName, func(event *application.CustomEvent) {
		// This is called in a goroutine
		counter.Add(1)
		wg.Done()
	})
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	wg.Wait()
	i.Equal(1, int(counter.Load()))

	// Unregister
	notifier.Reset()
	unregisterFn()
	counter.Store(0)
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	i.Equal(0, int(counter.Load()))

}
func Test_EventsOnMultiple(t *testing.T) {
	i := is.New(t)
	notifier := &mockNotifier{}
	eventProcessor := application.NewWailsEventProcessor(notifier.dispatchEventToWindows)
	t.Cleanup(eventProcessor.Close)

	// Test OnApplicationEvent
	eventName := "test"
	var counter atomic.Int32
	var wg sync.WaitGroup
	wg.Add(2)
	unregisterFn := eventProcessor.OnMultiple(eventName, func(event *application.CustomEvent) {
		// This is called in a goroutine
		counter.Add(1)
		wg.Done()
	}, 2)
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	wg.Wait()
	i.Equal(2, int(counter.Load()))

	// Unregister
	notifier.Reset()
	unregisterFn()
	counter.Store(0)
	_ = eventProcessor.Emit(&application.CustomEvent{
		Name: "test",
		Data: "test payload",
	})
	i.Equal(0, int(counter.Load()))

}

func TestEventProcessorDispatchesWindowEventsInEmitOrder(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	dispatched := make(chan struct{}, 2)

	var lock sync.Mutex
	var order []int
	processor := application.NewWailsEventProcessor(func(event *application.CustomEvent) {
		value := event.Data.(int)
		switch value {
		case 1:
			close(firstStarted)
			<-releaseFirst
		case 2:
			close(secondStarted)
		}

		lock.Lock()
		order = append(order, value)
		lock.Unlock()
		dispatched <- struct{}{}
	})
	t.Cleanup(processor.Close)

	_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 1})
	<-firstStarted
	_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 2})

	select {
	case <-secondStarted:
		t.Fatal("second window event overtook the first")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirst)
	<-dispatched
	<-dispatched

	lock.Lock()
	defer lock.Unlock()
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Fatalf("window event order = %v, want [1 2]", order)
	}
}

func TestEventProcessorSerialisesEachListenerIndependently(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstListener := make(chan int, 2)
	secondListener := make(chan int, 2)
	processor := application.NewWailsEventProcessor(func(*application.CustomEvent) {})
	t.Cleanup(processor.Close)

	processor.On("test", func(event *application.CustomEvent) {
		value := event.Data.(int)
		if value == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		firstListener <- value
	})
	processor.On("test", func(event *application.CustomEvent) {
		secondListener <- event.Data.(int)
	})

	_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 1})
	<-firstStarted
	_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 2})

	if got := <-secondListener; got != 1 {
		t.Fatalf("second listener received %d first", got)
	}
	if got := <-secondListener; got != 2 {
		t.Fatalf("second listener received %d second", got)
	}
	select {
	case got := <-firstListener:
		t.Fatalf("blocked listener completed %d", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)
	if got := <-firstListener; got != 1 {
		t.Fatalf("first listener received %d first", got)
	}
	if got := <-firstListener; got != 2 {
		t.Fatalf("first listener received %d second", got)
	}
}

func TestEventProcessorAllowsReentrantEmit(t *testing.T) {
	received := make(chan int, 2)
	processor := application.NewWailsEventProcessor(func(*application.CustomEvent) {})
	t.Cleanup(processor.Close)

	processor.On("test", func(event *application.CustomEvent) {
		value := event.Data.(int)
		received <- value
		if value == 1 {
			_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 2})
		}
	})
	_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 1})

	if got := <-received; got != 1 {
		t.Fatalf("received %d first", got)
	}
	if got := <-received; got != 2 {
		t.Fatalf("received %d second", got)
	}
}

func TestEventProcessorOnlyHooksCancelFanout(t *testing.T) {
	listenerCalled := make(chan struct{}, 1)
	frontendCalled := make(chan struct{}, 1)
	processor := application.NewWailsEventProcessor(func(*application.CustomEvent) {
		frontendCalled <- struct{}{}
	})
	t.Cleanup(processor.Close)
	processor.On("test", func(*application.CustomEvent) {
		listenerCalled <- struct{}{}
	})
	processor.RegisterHook("test", func(event *application.CustomEvent) {
		event.Cancel()
	})

	if err := processor.Emit(&application.CustomEvent{Name: "test"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-listenerCalled:
		t.Fatal("hook-cancelled event reached a Go listener")
	default:
	}
	select {
	case <-frontendCalled:
		t.Fatal("hook-cancelled event reached the frontend")
	default:
	}
}

func TestEventProcessorIgnoresCancellationFromAsyncListener(t *testing.T) {
	frontendCalled := make(chan struct{})
	processor := application.NewWailsEventProcessor(func(*application.CustomEvent) {
		close(frontendCalled)
	})
	t.Cleanup(processor.Close)
	processor.On("test", func(event *application.CustomEvent) {
		event.Cancel()
	})

	if err := processor.Emit(&application.CustomEvent{Name: "test"}); err != nil {
		t.Fatal(err)
	}
	<-frontendCalled
}

func TestEventProcessorUnsubscribeDrainsAssignedEvents(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	received := make(chan int, 2)
	routed := make(chan int, 2)
	processor := application.NewWailsEventProcessor(func(event *application.CustomEvent) {
		routed <- event.Data.(int)
	})
	t.Cleanup(processor.Close)
	unregister := processor.On("test", func(event *application.CustomEvent) {
		value := event.Data.(int)
		if value == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		received <- value
	})

	_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 1})
	<-firstStarted
	<-routed
	_ = processor.Emit(&application.CustomEvent{Name: "test", Data: 2})
	<-routed
	unregister()
	close(releaseFirst)

	if got := <-received; got != 1 {
		t.Fatalf("first assigned event = %d", got)
	}
	if got := <-received; got != 2 {
		t.Fatalf("second assigned event = %d", got)
	}
}
