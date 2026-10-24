package util

import (
	"bytes"
	"go/format"
	"log"
	"os"
	"text/template"
)

func GenerateSource(templateFile string, outputFile string, data any, formatSource bool) {
	// Read the template
	tmpl, err := template.ParseFiles(templateFile)
	if err != nil {
		log.Fatal(err)
	}

	// Execute the template with the input data
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, data); err != nil {
		log.Fatal(err)
	}

	var output []byte
	if formatSource {
		// Format the resulting source
		output, err = format.Source(buf.Bytes())
		if err != nil {
			log.Fatal(err)
		}
	} else {
		output = buf.Bytes()
	}

	// Write the new source file
	if err = os.WriteFile(outputFile, output, 0644); err != nil {
		log.Fatal(err)
	}
}
