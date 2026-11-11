//go:build ignore

package main

import (
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
)

const maxChannels = 100

type genData struct {
	Size    int
	Indices []int
}

func main() {
	// Generate the input data for the template
	data := make([]genData, maxChannels)
	for i := range maxChannels {
		indices := make([]int, i+1)
		for j := range indices {
			indices[j] = j
		}

		data[i] = genData{
			Size:    i + 1,
			Indices: indices,
		}
	}

	util.GenerateSource("select_template.tmpl", "select_generated.go", data, true)
}
