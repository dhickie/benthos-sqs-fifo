//go:build ignore

package main

import (
	"bytes"
	"go/format"
	"log"
	"os"
	"text/template"
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

	// Read the template
	tmpl, err := template.ParseFiles("select_template.tmpl")
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
	if err = os.WriteFile("select_generated.go", source, 0644); err != nil {
		log.Fatal(err)
	}
}
