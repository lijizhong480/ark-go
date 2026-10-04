package main

import (
	"flag"
	"fmt"
	"os"

	arktemplate "github.com/lijizhong480/ark-go/template"
)

func main() {
	name := flag.String("name", "", "service name, e.g. order")
	prefix := flag.String("module-prefix", "", "module path prefix, e.g. example.com/acme")
	output := flag.String("output", "", "new output directory")
	flag.Parse()
	if err := arktemplate.Generate(arktemplate.Options{Name: *name, ModulePrefix: *prefix, Output: *output}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
