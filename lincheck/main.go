package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

type Input struct {
	Op    string // "read" or "write"
	Key   string
	Value string // for write: value; for read: empty
}

// Specification for read-write register (state is last written value for each key)
var registerModel = porcupine.Model{
	// Initial state is empty string
	Init: func() any {
		return ""
	},

	// Step function: given current state + an operation, either accept and return next state, or reject.
	Step: func(state any, input any, output any) (bool, any) {
		cur := state.(string)
		in := input.(Input)

		switch in.Op {
		case "write":
			// Expected result is "ok" for writes
			outStr, _ := output.(string)
			if outStr != "ok" {
				return false, state
			}
			return true, in.Value

		case "read":
			outStr, _ := output.(string)
			// Expected result is the current value for reads
			if outStr != cur {
				return false, state
			}
			return true, state

		default:
			return false, state
		}
	},

	// Partition by key (so each key is checked independently).
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		m := map[string][]porcupine.Operation{}
		for _, op := range history {
			in := op.Input.(Input)
			m[in.Key] = append(m[in.Key], op)
		}
		out := make([][]porcupine.Operation, 0, len(m))
		for _, ops := range m {
			out = append(out, ops)
		}
		return out
	},
}

func main() {
	// Usage: `go run main.go history1.csv [history2.csv ...]`
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s history1.csv [history2.csv ...]\n", os.Args[0])
		os.Exit(2)
	}

	// Load and combine operations from all provided CSV files
	var ops []porcupine.Operation
	for _, path := range os.Args[1:] {
		fileOps, err := loadCSV(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load csv %s: %v\n", path, err)
			os.Exit(2)
		}
		ops = append(ops, fileOps...)
	}

	// Check linearizability
	ok := porcupine.CheckOperations(registerModel, ops)

	if ok {
		fmt.Println("✅ Linearizable (for the per-key register model).")
		os.Exit(0)
	}

	// If not linearizable, output HTML visualisation to help diagnose the violation
	fmt.Println("❌ NOT linearizable.")
	res, info := porcupine.CheckOperationsVerbose(registerModel, ops, 0)
	fmt.Println("Result:", res)

	const vizFile = "lincheck-viz.html"
	if f, err := os.Create(vizFile); err == nil {
		if vizErr := porcupine.Visualize(registerModel, info, f); vizErr != nil {
			fmt.Fprintln(os.Stderr, "warning: could not write visualisation:", vizErr)
		} else {
			fmt.Println("Visualisation written to", vizFile, "— open it in a browser.")
		}
		f.Close()
	} else {
		fmt.Fprintln(os.Stderr, "warning: could not create visualisation file:", err)
	}

	os.Exit(1)
}

func loadCSV(path string) ([]porcupine.Operation, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	r.TrimLeadingSpace = true

	// Read header
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	col := make(map[string]int, len(header))
	for i, h := range header {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}

	required := []string{"client", "op_id", "req_time", "res_time", "op_type", "key", "value", "result"}
	for _, k := range required {
		if _, ok := col[k]; !ok {
			return nil, fmt.Errorf("missing required column %q in header %v", k, header)
		}
	}

	// Construct history of operations from CSV records
	var ops []porcupine.Operation
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		clientStr := rec[col["client"]]
		opID := rec[col["op_id"]]
		clientID, err := strconv.Atoi(clientStr)
		if err != nil {
			return nil, fmt.Errorf("bad client id %q: %w", clientStr, err)
		}
		client := clientStr
		callStr := rec[col["req_time"]]
		retStr := rec[col["res_time"]]
		op := strings.ToLower(strings.TrimSpace(rec[col["op_type"]]))
		key := rec[col["key"]]
		arg := rec[col["value"]]
		result := rec[col["result"]]

		// Skip in-flight operations that never received a response.
		if callStr == "" || retStr == "" {
			continue
		}

		call, err := strconv.ParseInt(callStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad call time %q: %w", callStr, err)
		}
		ret, err := strconv.ParseInt(retStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad ret time %q: %w", retStr, err)
		}
		if ret < call {
			return nil, fmt.Errorf("ret < call for client=%s op_id=%s (%d < %d)", client, opID, ret, call)
		}

		in := Input{Op: op, Key: key}
		switch op {
		case "write":
			in.Value = arg
			ops = append(ops, porcupine.Operation{
				ClientId: clientID,
				Input:    in,
				Call:     call,
				Output:   "ok",
				Return:   ret,
			})
		case "read":
			ops = append(ops, porcupine.Operation{
				ClientId: clientID,
				Input:    in,
				Call:     call,
				Output:   result,
				Return:   ret,
			})
		default:
			return nil, fmt.Errorf("unknown op %q (expected read/write)", op)
		}
	}

	return ops, nil
}
