package mailbox

import (
	"sync"
	"testing"
)

func TestMailboxProcessesMessagesInOrder(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	consumed := make(chan int, 3)

	messages := New(func(message int) {
		if message == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		consumed <- message
	})

	messages.Send(1)
	<-firstStarted
	messages.Send(2)
	messages.Send(3)
	close(releaseFirst)
	messages.Close()

	for want := 1; want <= 3; want++ {
		if got := <-consumed; got != want {
			t.Fatalf("message %d consumed as %d", want, got)
		}
	}
	<-messages.Done()
}

func TestMailboxAcceptsConcurrentSenders(t *testing.T) {
	const count = 1000

	consumed := make(chan int, count)
	messages := New(func(message int) {
		consumed <- message
	})

	var senders sync.WaitGroup
	senders.Add(count)
	for message := range count {
		go func() {
			defer senders.Done()
			messages.Send(message)
		}()
	}
	senders.Wait()
	messages.Close()

	seen := make([]bool, count)
	for range count {
		message := <-consumed
		if seen[message] {
			t.Fatalf("message %d consumed twice", message)
		}
		seen[message] = true
	}
	<-messages.Done()
}

func TestMailboxCloseDrainsPendingMessages(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	consumed := make(chan int, 2)
	messages := New(func(message int) {
		if message == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		consumed <- message
	})

	messages.Send(1)
	<-firstStarted
	messages.Send(2)
	messages.Close()
	if messages.Send(3) {
		t.Fatal("closed mailbox accepted a message")
	}
	close(releaseFirst)
	<-messages.Done()

	if got := <-consumed; got != 1 {
		t.Fatalf("first message consumed as %d", got)
	}
	if got := <-consumed; got != 2 {
		t.Fatalf("second message consumed as %d", got)
	}
}

func TestMailboxStopDiscardsPendingMessages(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	consumed := make(chan int, 2)
	messages := New(func(message int) {
		if message == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		consumed <- message
	})

	messages.Send(1)
	<-firstStarted
	messages.Send(2)
	messages.Stop()
	close(releaseFirst)
	<-messages.Done()

	if got := <-consumed; got != 1 {
		t.Fatalf("first message consumed as %d", got)
	}
	select {
	case got := <-consumed:
		t.Fatalf("stopped mailbox consumed %d", got)
	default:
	}
}

func TestMailboxStopEscalatesDrainingClose(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	consumed := make(chan int, 2)
	messages := New(func(message int) {
		if message == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		consumed <- message
	})

	messages.Send(1)
	<-firstStarted
	messages.Send(2)
	messages.Close()
	messages.Stop()
	close(releaseFirst)
	<-messages.Done()

	if got := <-consumed; got != 1 {
		t.Fatalf("in-flight message consumed as %d", got)
	}
	select {
	case got := <-consumed:
		t.Fatalf("stopped draining mailbox consumed %d", got)
	default:
	}
}
