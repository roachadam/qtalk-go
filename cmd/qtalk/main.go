package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"

	"github.com/progrium/clon-go"
	"github.com/roachadam/qtalk-go/codec"
	"github.com/roachadam/qtalk-go/talk"
)

func main() {
	flag.Parse()

	cmd := flag.Arg(0)
	// if cmd != "call" {
	// 	log.Fatal("unknown command")
	// 	return
	// }
	fmt.Println(cmd)
	return

	u, err := url.Parse(flag.Arg(1))
	if err != nil {
		log.Fatal(err)
	}

	var args any
	if len(flag.Args()) > 2 {
		args, err = clon.Parse(flag.Args()[2:])
		if err != nil {
			log.Fatal(err)
		}
	}

	peer, err := talk.Dial(u.Scheme, u.Host, codec.JSONCodec{})
	if err != nil {
		log.Fatal(err)
	}
	defer peer.Close()

	var ret any
	_, err = peer.Call(context.Background(), u.Path, args, &ret)
	if err != nil {
		log.Fatal(err)
	}

	b, err := json.MarshalIndent(ret, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(b))
}
