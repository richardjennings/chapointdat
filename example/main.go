package main

import (
	ch "chapointdat"
	"fmt"
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
			fmt.Println(p)
			return nil
		},
		func(c ch.Company) error {
			fmt.Println(c)
			return nil
		},
		func(h ch.Header) error {
			fmt.Println(h)
			return nil
		},
		func(f ch.Footer) error {
			fmt.Println(f)
			return nil
		},
	)
	if err := r.Extract(filePath); err != nil {
		log.Fatal(err)
	}
}
