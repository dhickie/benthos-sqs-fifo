package flow

import (
	"cmp"
	"context"
	"dhickie/benthos-sqs-fifo/src/input/internal/test"
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
)

var rng = test.NewRng(0)

func TestSendAndReceive_SendsAndReceivesTheSameMessage(t *testing.T) {
	// Arrange
	ch := makePipe(1, time.Duration(1)*time.Minute)
	defer ch.Close(t.Context())
	m := buildMessage()

	// Act
	if err := ch.Send(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	r, err := ch.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	assert.Equal(t, 1, len(r))
	rM := r[0]
	assert.Equal(t, m.intValue, rM.intValue)
	assert.Equal(t, m.int32Value, rM.int32Value)
	assert.Equal(t, m.strValue, rM.strValue)
	assert.Equal(t, m.floatValue, rM.floatValue)
	assert.Equal(t, m.boolValue, rM.boolValue)
	assert.Equal(t, m.timeValue, rM.timeValue)
}

func TestReceive_ReceivesBatchedInput(t *testing.T) {
	// Arrange
	bSize := 10
	ch := makePipe(bSize, time.Duration(1)*time.Minute)
	defer ch.Close(t.Context())
	m := buildMessages(bSize)
	for i := range bSize {
		if err := ch.Send(t.Context(), m[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Act
	r, err := ch.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	assert.Equal(t, bSize, len(r))
}

func TestReceive_ReceivesUnfilledBatchAfterBatchPeriod(t *testing.T) {
	// Arrange
	bSize := 10
	ch := makePipe(bSize, time.Duration(1)*time.Millisecond)
	defer ch.Close(t.Context())
	m := buildMessage()
	if err := ch.Send(t.Context(), m); err != nil {
		t.Fatal(err)
	}

	// Act
	r, err := ch.Receive(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	// Assert
	assert.Equal(t, 1, len(r))
}

func TestClose_WaitsForPipeToBeEmpty(t *testing.T) {
	// Arrange
	bSize := 1
	nM := 2
	ch := makePipe(bSize, time.Duration(1)*time.Minute)
	m := buildMessages(nM)
	for i := range nM {
		if err := ch.Send(t.Context(), m[i]); err != nil {
			t.Fatal(err)
		}
	}

	// Act
	wg := startCloseRoutine(t.Context(), t, ch)
	rM := make([]*TestMessage, 0)
	for range nM {
		if r, err := ch.Receive(t.Context()); err != nil {
			t.Fatal(err)
		} else {
			rM = append(rM, r...)
		}
	}
	wg.Wait()

	// Assert
	assert.Equal(t, nM, len(rM))
}

func TestClose_ForceClosesPipeIfContextIsCancelled(t *testing.T) {
	// Arrange
	ctx, cancel := context.WithCancel(t.Context())
	ch := makePipe(1, time.Duration(1)*time.Minute)
	m := buildMessage()
	if err := ch.Send(t.Context(), m); err != nil {
		cancel()
		t.Fatal(err)
	}

	// Act & Assert
	wg := startCloseRoutine(ctx, t, ch)
	cancel()
	wg.Wait()
}

func TestPipe_UnderHighLoad(t *testing.T) {
	const nPipes = 100
	const nMsgsPerPipe = 10000
	const batchSize = 10
	const batchPeriod = time.Duration(1) * time.Second

	// Create the pipes
	bp := NewBatchPolicy(batchSize, batchPeriod)
	pipes := make([]*Pipe[TestMessage], nPipes)
	for i := range nPipes {
		pipes[i] = NewPipe[TestMessage](bp)
	}

	// Start publishing & subscribing routines
	wg := &sync.WaitGroup{}
	sendErrs := make([]error, nPipes)
	receiveErrs := make([]error, nPipes)
	startTimes := make([]time.Time, nPipes)
	durations := make([]time.Duration, nPipes)
	latencies := make([][]float64, nPipes)
	publishFunc := func(i int) {
		defer wg.Done()
		msgs := buildMessages(nMsgsPerPipe)
		startTimes[i] = time.Now()
		for j := range nMsgsPerPipe {
			msgs[j].timeValue = time.Now()
			if err := pipes[i].Send(t.Context(), msgs[j]); err != nil {
				sendErrs[i] = err
				return
			}
		}
	}
	receiveFunc := func(i int) {
		defer wg.Done()
		msgs := make([]*TestMessage, nMsgsPerPipe)
		latencies[i] = make([]float64, nMsgsPerPipe)
		received := 0
		for received < nMsgsPerPipe {
			if batch, err := pipes[i].Receive(t.Context()); err != nil {
				receiveErrs[i] = err
				return
			} else {
				for _, v := range batch {
					latencies[i][received] = float64(time.Since(v.timeValue).Nanoseconds())
					msgs[received] = v
					received++
				}
			}
		}
		durations[i] = time.Since(startTimes[i])
	}
	for i := range nPipes {
		wg.Add(2)
		go receiveFunc(i)
		go publishFunc(i)
	}

	// Wait for the sendind/receiving to finish
	wg.Wait()

	// Check for errors and calculate the number of messages published per second
	errored := slices.ContainsFunc(sendErrs, func(e error) bool {
		return e != nil
	})
	if errored {
		t.Fatal("Errors were returned while trying to send messages")
	}
	errored = slices.ContainsFunc(receiveErrs, func(e error) bool {
		return e != nil
	})
	if errored {
		t.Fatal("Errors were returned while trying to receive messages")
	}

	rates := make([]float64, nPipes)
	for i := range nPipes {
		rates[i] = (time.Second.Seconds() / durations[i].Seconds()) * nMsgsPerPipe
	}

	minRate := slices.Min(rates)
	minLatency := minMany(latencies)
	maxRate := slices.Max(rates)
	maxLatency := maxMany(latencies)
	meanRate := mean(rates)
	meanLatency := meanMany(latencies)

	fmt.Printf("Min rate: %.2f\n", minRate)
	fmt.Printf("Max rate: %.2f\n", maxRate)
	fmt.Printf("Mean rate: %.2f\n", meanRate)
	fmt.Printf("Min latency: %.2fns\n", minLatency)
	fmt.Printf("Max latency: %.2fns\n", maxLatency)
	fmt.Printf("Mean latency: %.2fns\n", meanLatency)
}

func minMany[T cmp.Ordered](values [][]T) T {
	l := make([]T, len(values))
	for i := range values {
		l[i] = slices.Min(values[i])
	}
	return slices.Min(l)
}

func maxMany[T cmp.Ordered](values [][]T) T {
	l := make([]T, len(values))
	for i := range values {
		l[i] = slices.Max(values[i])
	}
	return slices.Max(l)
}

func mean(values []float64) float64 {
	total := 0.0
	for _, v := range values {
		total += v
	}
	return total / float64(len(values))
}

func meanMany(values [][]float64) float64 {
	total := 0.0
	n := 0
	for _, v := range values {
		for _, v1 := range v {
			total += v1
			n++
		}
	}
	return total / float64(n)
}

func startCloseRoutine(ctx context.Context, t *testing.T, ch *Pipe[TestMessage]) *sync.WaitGroup {
	wg := sync.WaitGroup{}
	c := util.NewAsyncCond()
	wg.Go(func() {
		c.Signal()
		ch.Close(ctx)
	})
	if err := c.Wait(ctx); err != nil { // Wait for the goroutine to start
		t.Fatal(err)
	}

	return &wg
}

func makePipe(batchSize int, period time.Duration) *Pipe[TestMessage] {
	bp := NewBatchPolicy(batchSize, period)
	return NewPipe[TestMessage](bp)
}

func buildMessages(n int) []*TestMessage {
	m := make([]*TestMessage, n)
	for i := range n {
		m[i] = buildMessage()
	}

	return m
}

func buildMessage() *TestMessage {
	return &TestMessage{
		intValue:   rng.RandRange(1, 1000),
		int32Value: int32(rng.RandRange(1, 1000)),
		floatValue: rng.RandFloatRange(1, 1000, 3),
		strValue:   uuid.New().String(),
		boolValue:  rng.RandBool(),
		timeValue:  time.Now(),
	}
}

type TestMessage struct {
	intValue   int
	int32Value int32
	floatValue float32
	strValue   string
	boolValue  bool
	timeValue  time.Time
}
