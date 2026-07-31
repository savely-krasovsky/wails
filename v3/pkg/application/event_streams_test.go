package application

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wailsapp/wails/v3/internal/mailbox"
	"github.com/wailsapp/wails/v3/pkg/events"
)

func TestApplicationEventListenersAreSerialAndIndependent(t *testing.T) {
	app := newEventTestApp()
	defer stopEventTestApp(app)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstListener := make(chan int, 2)
	secondListener := make(chan int, 2)
	var blockFirst sync.Once
	const eventType events.ApplicationEventType = 999_001

	app.Event.OnApplicationEvent(eventType, func(event *ApplicationEvent) {
		blockFirst.Do(func() {
			close(firstStarted)
			<-releaseFirst
		})
		firstListener <- event.Context().Data()["sequence"].(int)
	})
	app.Event.OnApplicationEvent(eventType, func(event *ApplicationEvent) {
		secondListener <- event.Context().Data()["sequence"].(int)
	})

	firstContext := newApplicationEventContext()
	firstContext.setData(map[string]any{"sequence": 1})
	secondContext := newApplicationEventContext()
	secondContext.setData(map[string]any{"sequence": 2})
	app.applicationEvents.Send(&ApplicationEvent{Id: uint(eventType), ctx: firstContext})
	<-firstStarted
	app.applicationEvents.Send(&ApplicationEvent{Id: uint(eventType), ctx: secondContext})

	if got := <-secondListener; got != 1 {
		t.Fatalf("independent listener received %d first", got)
	}
	if got := <-secondListener; got != 2 {
		t.Fatalf("independent listener received %d second", got)
	}
	select {
	case <-firstListener:
		t.Fatal("blocked application listener completed early")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	if got := <-firstListener; got != 1 {
		t.Fatalf("blocked listener received %d first", got)
	}
	if got := <-firstListener; got != 2 {
		t.Fatalf("blocked listener received %d second", got)
	}
}

func TestApplicationEventHookCancelsListenerAssignment(t *testing.T) {
	app := newEventTestApp()
	defer stopEventTestApp(app)

	called := make(chan struct{}, 1)
	frontend := make(chan *CustomEvent, 1)
	hookRan := make(chan struct{})
	app.addWailsEventListener(wailsEventListenerFunc(func(event *CustomEvent) {
		frontend <- event
	}))
	const eventType events.ApplicationEventType = 999_002
	app.Event.RegisterApplicationEventHook(eventType, func(event *ApplicationEvent) {
		event.Cancel()
		close(hookRan)
	})
	app.Event.OnApplicationEvent(eventType, func(*ApplicationEvent) {
		called <- struct{}{}
	})
	app.applicationEvents.Send(&ApplicationEvent{Id: uint(eventType), ctx: newApplicationEventContext()})
	<-hookRan

	select {
	case <-called:
		t.Fatal("cancelled application event reached a listener")
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-frontend:
		t.Fatal("cancelled application event reached the frontend")
	default:
	}
}

func TestApplicationEventsReachFrontendInOrder(t *testing.T) {
	app := newEventTestApp()
	defer stopEventTestApp(app)

	frontend := make(chan string, 2)
	app.addWailsEventListener(wailsEventListenerFunc(func(event *CustomEvent) {
		frontend <- event.Name
	}))
	app.applicationEvents.Send(&ApplicationEvent{Id: 999_010, ctx: newApplicationEventContext()})
	app.applicationEvents.Send(&ApplicationEvent{Id: 999_011, ctx: newApplicationEventContext()})

	if got := <-frontend; got != events.JSEvent(999_010) {
		t.Fatalf("first frontend application event = %q", got)
	}
	if got := <-frontend; got != events.JSEvent(999_011) {
		t.Fatalf("second frontend application event = %q", got)
	}
}

func TestWindowEventListenersAreSerialAndIndependent(t *testing.T) {
	window := NewWindow(WebviewWindowOptions{})
	defer window.markAsDestroyed()

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstListener := make(chan struct{}, 2)
	secondListener := make(chan struct{}, 2)
	var calls atomic.Int32
	const eventType events.WindowEventType = 999_003

	window.OnWindowEvent(eventType, func(*WindowEvent) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		} else {
			close(secondStarted)
		}
		firstListener <- struct{}{}
	})
	window.OnWindowEvent(eventType, func(*WindowEvent) {
		secondListener <- struct{}{}
	})

	window.HandleWindowEvent(uint(eventType))
	<-firstStarted
	window.HandleWindowEvent(uint(eventType))
	<-secondListener
	<-secondListener
	select {
	case <-secondStarted:
		t.Fatal("second window callback started before the first completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirst)
	<-firstListener
	<-secondStarted
	<-firstListener
}

func TestWindowEventHookCancelsListenerAssignment(t *testing.T) {
	window := NewWindow(WebviewWindowOptions{})
	defer window.markAsDestroyed()

	called := make(chan struct{}, 1)
	hookRan := make(chan struct{})
	const eventType events.WindowEventType = 999_004
	window.RegisterHook(eventType, func(event *WindowEvent) {
		event.Cancel()
		close(hookRan)
	})
	window.OnWindowEvent(eventType, func(*WindowEvent) {
		called <- struct{}{}
	})
	window.HandleWindowEvent(uint(eventType))
	<-hookRan

	select {
	case <-called:
		t.Fatal("cancelled window event reached a listener")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestCustomEventListenerContinuesAfterHandledPanic(t *testing.T) {
	app := Get()
	if app == nil {
		app = New(Options{})
	}
	previousHandler := app.options.PanicHandler
	panicHandled := make(chan struct{}, 1)
	app.options.PanicHandler = func(*PanicDetails) {
		panicHandled <- struct{}{}
	}
	t.Cleanup(func() {
		app.options.PanicHandler = previousHandler
	})

	processor := NewWailsEventProcessor(func(*CustomEvent) {})
	defer processor.Close()
	continued := make(chan struct{})
	processor.On("test", func(event *CustomEvent) {
		if event.Data == 1 {
			panic("handled")
		}
		close(continued)
	})
	processor.Emit(&CustomEvent{Name: "test", Data: 1})
	processor.Emit(&CustomEvent{Name: "test", Data: 2})

	<-panicHandled
	<-continued
}

func newEventTestApp() *App {
	app := &App{
		applicationEventHooks:     make(map[uint][]*eventHook),
		applicationEventListeners: make(map[uint][]*EventListener),
	}
	app.Event = newEventManager(app)
	app.applicationEvents = mailbox.New(app.Event.handleApplicationEvent)
	return app
}

func stopEventTestApp(app *App) {
	app.applicationEvents.Stop()
	app.applicationEventListenersLock.Lock()
	defer app.applicationEventListenersLock.Unlock()
	for _, listeners := range app.applicationEventListeners {
		for _, listener := range listeners {
			listener.events.Stop()
		}
	}
	for _, transport := range app.wailsEventListeners {
		transport.events.Stop()
	}
}

type wailsEventListenerFunc func(*CustomEvent)

func (f wailsEventListenerFunc) DispatchWailsEvent(event *CustomEvent) {
	f(event)
}
