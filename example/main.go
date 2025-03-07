package main

import (
	"fmt"
	ch "github.com/richardjennings/chapointdat"
	"log"
	"os"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: go run example/main.go <file.zip>")
	}
	filePath := os.Args[1]
	r := ch.NewReader(
		func(p ch.Person) error {
			fmt.Printf("Person: %v\n", p)
			return nil
		},
		func(c ch.Company) error {
			fmt.Printf("Company: %v\n", c)
			return nil
		},
		func(h ch.Header) error {
			fmt.Printf("Header: %v\n", h)
			return nil
		},
		func(f ch.Footer) error {
			fmt.Printf("Footer: %v\n", f)
			return nil
		},
	)
	errH := func(err error) {
		log.Println(err)
	}
	if err := r.Extract(filePath, errH); err != nil {
		log.Fatal(err)
	}
}
