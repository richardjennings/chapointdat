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
	opts := []ch.Opt{
		ch.WithPersonHandler(
			func(p ch.Person) error {
				fmt.Printf("Person: %v\n", p)
				return nil
			},
		),
		ch.WithCompanyHandler(
			func(c ch.Company) error {
				fmt.Printf("Company: %v\n", c)
				return nil
			},
		),
		ch.WithHeaderHandler(
			func(h ch.Header) error {
				fmt.Printf("Header: %v\n", h)
				return nil
			},
		),
		ch.WithFooterHandler(
			func(f ch.Footer) error {
				fmt.Printf("Footer: %v\n", f)
				return nil
			},
		),
	}
	r := ch.NewReader(opts...)
	errH := func(err error) {
		log.Println(err)
	}
	if err := r.Extract(filePath, 1, errH); err != nil {
		log.Fatal(err)
	}
}
