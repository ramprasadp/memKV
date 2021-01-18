package main

import (
	"fmt"
	"os"

	"../memkvlib"
)

func main() {
	fmt.Printf(" Testing setex on memkv")
	addr := "127.0.0.1:9980"

	mdbh, err := memkvlib.Connect(addr)
	if err != nil {
		fmt.Printf(" Could not connect to memkv %s\n", err)
		os.Exit(1)
	}

	reply, err := mdbh.Setex("first", "1", 40)
	reply, err = mdbh.Setex("Second", "1", 40)
	reply, err = mdbh.Setex("Third", "1", 40)

	fmt.Printf(" Got reply from memkv %s\n", reply)
}
