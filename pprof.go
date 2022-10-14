package main

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/richardartoul/molecule"
	"github.com/richardartoul/molecule/src/protowire"
)

type Breakdown struct {
	// Timestamps is a sequence of timestamps in nanoseconds
	// when the samples occured
	Timestamps []int64
	Values     []int64
	LabelSets  []int64
}

type PprofInfo struct {
	// Value is the sum of all Values in Breakdown
	Value int64
	// Breakdown shows the individual timestamped events
	Breakdown Breakdown
}

type LabelSet struct {
	ID     int64
	Labels []string
}

// ToPprof converts CPU profile samples in a runtime execution trace into a
// pprof-encoded profile.
//
// The profile also includes Felix's proposed "Breakdown" field for the
// samples. The new format also introduces a LabelSet, which identifies a
// repeated collection of labels. The Breakdown field shows the individual
// timestamped events which make up the overall sample. Each event in a
// breakdown also has an associated label set, which includes a label for which
// goroutine was running.
func ToPprof(parsed ParseResult, start, stop time.Time, out io.Writer) error {
	info := make(map[uint64]*PprofInfo)
	// labelSetIDs associates the same set of labels
	// (just concatenating all the strings) with the ID of that label set
	labelSetIDs := make(map[string]*LabelSet)
	// labelSets is the actual label sets
	var labelSets []*LabelSet
	for _, event := range parsed.Events {
		switch event.Type {
		case EvCPUSample:
			pp, ok := info[event.StkID]
			if !ok {
				pp = new(PprofInfo)
				info[event.StkID] = pp
			}
			value := int64(1)
			pp.Value += value
			bd := &pp.Breakdown
			bd.Timestamps = append(bd.Timestamps, event.Ts)
			bd.Values = append(bd.Values, value)
			labels := []string{
				"thread_id:",
				strconv.Itoa(int(event.G)),
				// TODO: pprof labels
				// The execution tracer doesn't track pprof labels.
				// See https://cs.opensource.google/go/go/+/master:src/runtime/trace.go;l=839-843;drc=7feb68728dda2f9d86c0a1158307212f5a4297ce;bpv=1;bpt=1
			}
			concat := new(strings.Builder)
			for _, l := range labels {
				concat.WriteString(l)
			}
			s := concat.String()
			set, ok := labelSetIDs[s]
			if !ok {
				set = &LabelSet{
					ID:     int64(len(labelSets)),
					Labels: labels,
				}
				labelSetIDs[s] = set
				labelSets = append(labelSets, set)
			}
			bd.LabelSets = append(bd.LabelSets, set.ID)
		}
	}
	for i, set := range labelSets {
		fmt.Printf("label set %d: %s\n", i, set.Labels)
	}
	for id, pp := range info {
		fmt.Printf("stack %d observed: value %d, breakdown %+v\n", id, pp.Value, pp.Breakdown)
		for _, frame := range parsed.Stacks[id] {
			fmt.Printf("\t%+v\n", frame)
		}
	}

	// BUILDING PPROF-ENCODED PROFILE

	buf := new(bytes.Buffer)
	strtab := make(StrTab)
	ps := molecule.NewProtoStream(buf)

	// Value type, 1
	ps.Embedded(1, func(ps *molecule.ProtoStream) error {
		ps.Int64(1, strtab.Get("time")) // type
		ps.Int64(2, strtab.Get("ns"))   // unit
		return nil
	})

	// LabelSet, 16
	for _, set := range labelSets {
		ps.Embedded(16, func(ps *molecule.ProtoStream) error {
			ps.Uint64(1, uint64(set.ID)) // id
			for i := 0; i < len(set.Labels); i += 2 {
				// label
				ps.Embedded(2, func(ps *molecule.ProtoStream) error {
					ps.Int64(1, strtab.Get(set.Labels[i]))   // key
					ps.Int64(2, strtab.Get(set.Labels[i+1])) // value
					return nil
				})
			}
			return nil
		})
	}

	// Samples, 2
	for id, pp := range info {
		ps.Embedded(2, func(ps *molecule.ProtoStream) error {
			stk := parsed.Stacks[id]
			for _, frame := range stk {
				ps.Uint64(1, frame.PC) // location ID
			}
			ps.Int64(2, pp.Value)
			// breakdown
			ps.Embedded(4, func(ps *molecule.ProtoStream) error {
				// TODO: delta-encode timestamps? make sure they're relative to start time
				ps.Int64Packed(1, pp.Breakdown.Timestamps)
				ps.Int64Packed(2, pp.Breakdown.Values)
				ps.Int64Packed(3, pp.Breakdown.LabelSets)
				return nil
			})
			return nil
		})
	}

	// Mapping, 3
	ps.Embedded(3, func(ps *molecule.ProtoStream) error {
		ps.Uint64(1, 1) // mapping ID
		return nil
	})

	// Function, 5
	functions := make(map[string]uint64)
	for _, stk := range parsed.Stacks {
		for _, frame := range stk {
			concat := frame.Fn + frame.File
			id, ok := functions[concat]
			if ok {
				continue
			}
			id = uint64(len(functions) + 1)
			functions[concat] = id
			ps.Embedded(5, func(ps *molecule.ProtoStream) error {
				ps.Uint64(1, id)                    // unique ID
				ps.Int64(2, strtab.Get(frame.Fn))   // name
				ps.Int64(4, strtab.Get(frame.File)) // filename
				return nil
			})
		}
	}

	// Location, 4
	locs := make(map[uint64]struct{}) // so we don't duplicate
	for _, stk := range parsed.Stacks {
		for _, frame := range stk {
			pc := frame.PC
			if _, ok := locs[pc]; ok {
				continue
			}
			locs[pc] = struct{}{}
			ps.Embedded(4, func(ps *molecule.ProtoStream) error {
				concat := frame.Fn + frame.File
				id := functions[concat]
				ps.Uint64(1, pc) // ID
				ps.Uint64(2, 1)  // mapping ID
				ps.Uint64(3, pc) // address
				ps.Embedded(4, func(ps *molecule.ProtoStream) error {
					ps.Uint64(1, id)               // function ID
					ps.Int64(2, int64(frame.Line)) // line
					return nil
				})
				return nil
			})
		}
	}

	// Time nanos, 9
	ps.Int64(9, start.UnixNano())

	// Duration nanos, 10
	ps.Int64(10, stop.Sub(start).Nanoseconds())

	// Period type, 11
	ps.Embedded(11, func(ps *molecule.ProtoStream) error {
		// TODO: make this right
		ps.Int64(1, strtab.Get("time")) // type
		ps.Int64(2, strtab.Get("ns"))   // unit
		return nil
	})

	// Period, 12
	ps.Int64(12, 1)

	// Tick unit, 15
	ps.Int64(15, strtab.Get("nanoseconds"))

	// String table, 6
	// Have to write the string table manually because the first string
	// must be length 0, and molecule declines to write length-0 stuff
	b := buf.Bytes()
	writeString := func(s string) {
		b = protowire.AppendVarint(b, (6<<3)|2) // field, wire type
		b = protowire.AppendVarint(b, uint64(len(s)))
		b = append(b, s...)
	}
	writeString("")
	for s := range strtab {
		writeString(s)
	}

	//_, err := io.Copy(out, buf)
	_, err := out.Write(b)
	return err
}

// StrTab deduplicates strings, gives them unique IDs
type StrTab map[string]int64

func (t StrTab) Get(s string) int64 {
	id, ok := t[s]
	if !ok {
		id = int64(len(t))
		t[s] = id
	}
	return id
}
