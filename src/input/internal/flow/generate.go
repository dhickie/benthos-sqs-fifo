//go:build ignore

package main

import (
	"bytes"
	"go/format"
	"log"
	"os"
	"text/template"
)

const maxHandlers = 20

type genData struct {
	Size    int
	Indices []int
}

func main() {
	// Generate the input data for the template
	data := make([]genData, maxHandlers-1)
	for i := range maxHandlers - 1 {
		indices := make([]int, i+1)
		for j := range indices {
			indices[j] = j + 2
		}

		data[i] = genData{
			Size:    i + 2,
			Indices: indices,
		}
	}

	// Read the template
	tmpl, err := template.ParseFiles("component_process.tmpl")
	if err != nil {
		log.Fatal(err)
	}

	// Execute the template with the input data
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, data); err != nil {
		log.Fatal(err)
	}

	// Format the resulting source
	source, err := format.Source(buf.Bytes())
	if err != nil {
		log.Fatal(err)
	}

	// Write the new source file
	if err = os.WriteFile("component_process_gen.go", source, 0644); err != nil {
		log.Fatal(err)
	}
}
