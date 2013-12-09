package tally

import (
	"fmt"
	"io"
	"time"
)

const (
	STATGRAM_MAXSIZE = 10240
)

// Receivers share the work of listening on a UDP port and accumulating stats.
type Receiver struct {
	id               string // child identifier for collecting internal stats
	conn             io.Reader
	lastMessageCount int64
	messageCount     int64
	lastByteCount    int64
	byteCount        int64
	readBuf          []byte
	parser           *StatgramParser
}

func NewReceiver() *Receiver {
	return &Receiver{
		readBuf: make([]byte, STATGRAM_MAXSIZE),
		parser:  NewStatgramParser(),
	}
}

// ReadOnce blocks on the listening connection until a statgram arrives. It
// takes care of parsing it and returns it. Any parse errors are ignored, so
// it's possible an empty statgram will be returned.
func (receiver *Receiver) ReadOnce() (s Statgram, err error) {
	var size int
	size, err = receiver.conn.Read(receiver.readBuf)
	if err == nil {
		receiver.messageCount += 1
		receiver.byteCount += int64(size)
		s = receiver.parser.ParseStatgram(receiver.readBuf[:size])
	}
	return
}

// ReceiveStatgrams spins off a goroutine to read statgrams off the UDP port.
// Returns a buffered channel that will receive statgrams as they arrive.
func (receiver *Receiver) ReceiveStatgrams() (statgrams chan Statgram) {
	statgrams = make(chan Statgram, 0)

	// Use double-buffering to collect parsed statgrams. We copy into one,
	// then swap so we can send the other over the channel while copying in the
	// next statgram.
	processing1 := make(Statgram, STATGRAM_MAXSIZE)
	processing2 := make(Statgram, STATGRAM_MAXSIZE)

	go func() {
		for {
			statgram, err := receiver.ReadOnce()
			if err != nil {
				close(statgrams)
				break
			}

			copy(processing1, statgram)
			// swap the buffers so we don't overwrite the statgram being
			// processed with the next one we read
			processing1, processing2 = processing2, processing1
			statgrams <- processing2[:len(statgram)]
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
	receiver := NewReceiver()
	receiver.id = id
	receiver.conn = conn
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
		controlChannels = append(controlChannels,
			RunReceiver(fmt.Sprintf("%d", i), conn))
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
