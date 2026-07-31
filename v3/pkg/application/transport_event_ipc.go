package application

type EventIPCTransport struct {
	app *App
}

func (t *EventIPCTransport) DispatchWailsEvent(event *CustomEvent) {
	// Snapshot the window list under the lock. Each native window owns its
	// delivery worker, so enqueueing here never waits for another window's
	// WebView or runtime acknowledgement.
	t.app.windowsLock.RLock()
	windows := make([]Window, 0, len(t.app.windows))
	for _, w := range t.app.windows {
		windows = append(windows, w)
	}
	t.app.windowsLock.RUnlock()

	for _, window := range windows {
		window.DispatchWailsEvent(event)
	}
}
