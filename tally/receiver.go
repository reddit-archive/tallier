package tally

import (
	"io"
	"time"
)

const (
	STATGRAM_CHANNEL_BUFSIZE = 1024
	STATGRAM_MAXSIZE         = 1024
)

// Receivers share the work of listening on a UDP port and accumulating stats.
type Receiver struct {
	id               string // child identifier for collecting internal stats
	conn             io.Reader
	lastMessageCount int64
	messageCount     int64
	lastByteCount    int64
	byteCount        int64
}

// ReadOnce blocks on the listening connection until a statgram arrives. It
// takes care of parsing it and returns it. Any parse errors are ignored, so
// it's possible an empty statgram will be returned.
func (receiver *Receiver) ReadOnce() (statgram Statgram, err error) {
	buf := make([]byte, STATGRAM_MAXSIZE)
	var size int
	size, err = receiver.conn.Read(buf)
	if err == nil {
		receiver.messageCount += 1
		receiver.byteCount += int64(size)
		statgram = ParseStatgram(string(buf[:size]))
	}
	return
}

// ReceiveStatgrams spins off a goroutine to read statgrams off the UDP port.
// Returns a buffered channel that will receive statgrams as they arrive.
func (receiver *Receiver) ReceiveStatgrams() (statgrams chan Statgram) {
	statgrams = make(chan Statgram, STATGRAM_CHANNEL_BUFSIZE)
	go func() {
		for {
			statgram, err := receiver.ReadOnce()
			if err != nil {
				close(statgrams)
				break
			}
			statgrams <- statgram
		}
	}()
	return
}

// RunReceiver spins off a goroutine to receive and process statgrams. Returns a
// bidirectional control channel, which provides a snapshot each time it's given
// a nil value.
//
// An optional channel for notification of processed statgrams may be passed in
// to facilitate testing.
func RunReceiver(id string, conn io.Reader,
	notifiers ...chan Statgram) (controlChannel chan *Snapshot) {
	receiver := &Receiver{
		id:   id,
		conn: conn,
	}
	snapshot := NewSnapshot()
	controlChannel = make(chan *Snapshot)
	statgrams := receiver.ReceiveStatgrams()
	closed := false
	go func() {
		for {
			select {
			case statgram, ok := <-statgrams:
				if !ok {
					infolog("EOF received")
					closed = true
					if snapshot.NumStats() == 0 {
						close(controlChannel)
						break
					}
				}
				snapshot.ProcessStatgram(statgram)
				for _, notifier := range notifiers {
					notifier <- statgram
				}
			case _ = <-controlChannel:
				snapshot.Count("tallier.messages.child_"+receiver.id,
					float64(receiver.messageCount-receiver.lastMessageCount))
				snapshot.Count("tallier.bytes.child_"+receiver.id,
					float64(receiver.byteCount-receiver.lastByteCount))
				receiver.lastMessageCount = receiver.messageCount
				receiver.lastByteCount = receiver.byteCount
				controlChannel <- snapshot
				if closed {
					close(controlChannel)
					break
				} else {
					snapshot = NewSnapshot()
				}
			}
		}
	}()
	return
}

// Aggregate spins off receivers and a goroutine to manage them. Returns a
// channel to coordinate the collection of snapshots from the receivers.
func Aggregate(conn io.Reader, numReceivers int) (snapchan chan *Snapshot) {
	snapchan = make(chan *Snapshot)
	var controlChannels []chan *Snapshot
	for i := 0; i < numReceivers; i++ {
		controlChannels = append(controlChannels, RunReceiver(string(i), conn))
	}

	go func() {
		var numStats int64 = 0
		for {
			snapshot := <-snapchan
			for _, controlChannel := range controlChannels {
				controlChannel <- nil
			}
			for _, controlChannel := range controlChannels {
				snapshot.Aggregate(<-controlChannel)
			}
			snapshot.duration = time.Now().Sub(snapshot.start)
			numStats += int64(snapshot.NumStats())
			snapchan <- snapshot
		}
	}()
	return
}
