//go:build ignore

package main

import (
	"dhickie/benthos-sqs-fifo/src/input/internal/util"
)

const maxOutputTypes int = 10

type genData struct {
	NumOut     int
	GenIndexes []int
	OutIndexes []int
}

func main() {
	// Generate the input data for the template
	data := make([]genData, maxOutputTypes)
	for i := range maxOutputTypes {
		genIndexes := make([]int, i+1)
		outIndexes := make([]int, i)

		for j := range genIndexes {
			genIndexes[j] = j + 1
		}
		for j := range outIndexes {
			outIndexes[j] = j + 2
		}

		data[i] = genData{
			NumOut:     i + 1,
			GenIndexes: genIndexes,
			OutIndexes: outIndexes,
		}
	}

	util.GenerateSource("handler_template.tmpl", "handler_generated.go", data, true)
}
