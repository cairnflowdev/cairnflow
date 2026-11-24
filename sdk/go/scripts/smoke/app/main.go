// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// A user's app, built outside the repository by scripts/smoke.sh:
//
//	app freeport                       a free local port
//	app serve ADDR KID ORIGIN          serve SmokeCounter (verified: IdentityKeys, SegmentOrigins)
//	app call RUNTIME_URL API_KEY KEY   add 5 to KEY through the Go client, print the total
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	cairn "github.com/cairnflowdev/cairnflow/sdk/go"
	"github.com/cairnflowdev/cairnflow/sdk/go/client"
)

func add(ctx cairn.ObjectContext, n int) (int, error) {
	total, _, err := cairn.Get[int](ctx, "total")
	if err != nil {
		return 0, err
	}
	if err := cairn.Sleep(ctx, 10*time.Millisecond); err != nil {
		return 0, err
	}
	stamp, err := cairn.Run(ctx, "stamp", func(cairn.RunContext) (int, error) { return n, nil })
	if err != nil {
		return 0, err
	}
	total += stamp
	return total, cairn.Set(ctx, "total", total)
}

func get(ctx cairn.ObjectSharedContext) (int, error) {
	total, _, err := cairn.Get[int](ctx, "total")
	return total, err
}

var counter = cairn.NewObject("SmokeCounter",
	cairn.Exclusive("add", add),
	cairn.Shared("get", cairn.NoInput(get)))

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: app freeport | serve ADDR KID ORIGIN | call URL KEY OBJECT_KEY")
	}
	switch os.Args[1] {
	case "freeport":
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(ln.Addr().(*net.TCPAddr).Port)
		ln.Close()
	case "serve":
		log.Fatal(cairn.Serve(context.Background(), os.Args[2], counter,
			cairn.IdentityKeys(os.Args[3]), cairn.SegmentOrigins(os.Args[4])))
	case "call":
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		c := client.Connect(os.Args[2], client.WithAPIKey(os.Args[3]))
		obj := c.Object("SmokeCounter", os.Args[4])
		if _, err := client.Call[int](ctx, obj, "add", 5); err != nil {
			log.Fatal(err)
		}
		total, err := client.Call[int](ctx, obj, "get", nil)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(total)
	default:
		log.Fatalf("unknown command %q", os.Args[1])
	}
}
