package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"runtime/pprof"
	"runtime/trace"
	"sort"
	"sync"
)

type ParsedEvent struct {
	Type      string
	Goroutine uint64
	Timestamp int64
	Stack     []StackFrame
}

type StackFrame struct {
	Func string
	File string
	Line int
}

func main() {
	// start this so that we get CPU samples added to the trace
	// (requires Go >= 1.19)
	pprof.StartCPUProfile(io.Discard)
	defer pprof.StopCPUProfile()

	buf := new(bytes.Buffer)
	if err := trace.Start(buf); err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for j := 0; j < 4; j++ {
		wg.Add(1)
		// just do some work
		thingy := make([]int, 1_000_000)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				sort.Ints(thingy)
			}
		}()
	}
	wg.Wait()

	trace.Stop()

	if err := os.WriteFile("trace.out", buf.Bytes(), 0660); err != nil {
		panic(err)
	}

	res, err := Parse(buf, "")
	if err != nil {
		panic(err)
	}
	var stuff []ParsedEvent
	for _, event := range res.Events {
		eventType := EventDescriptions[event.Type]
		thing := ParsedEvent{
			Type:      eventType.Name,
			Timestamp: event.Ts,
			Goroutine: event.G,
		}
		stk := res.Stacks[event.StkID]
		for _, frame := range stk {
			thing.Stack = append(thing.Stack, StackFrame{
				File: frame.File,
				Func: frame.Fn,
				Line: frame.Line,
			})
		}
		stuff = append(stuff, thing)
	}
	buf.Reset()
	json.NewEncoder(buf).Encode(stuff)
	os.WriteFile("trace.json", buf.Bytes(), 0660)
}
