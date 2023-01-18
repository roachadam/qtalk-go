package rpc

import (
	"context"
	"io"
	"log"
	"net"

	"github.com/roachadam/qtalk-go/codec"
	"github.com/roachadam/qtalk-go/mux"
)

// Server wraps a Handler and codec to respond to RPC calls.
type Server struct {
	Handler Handler
	Codec   codec.Codec
	sess    mux.Session
}

// ServeMux will Accept sessions until the Listener is closed, and will Respond to accepted sessions in their own goroutine.
func (s *Server) ServeMux(l mux.Listener) error {
	for {
		sess, err := l.Accept()
		if err != nil {
			return err
		}
		go s.Respond(sess, nil)
	}
}

// Serve will Accept sessions until the Listener is closed, and will Respond to accepted sessions in their own goroutine.
func (s *Server) Serve(l net.Listener) error {
	return s.ServeMux(mux.ListenerFrom(l))
}

// Respond will Accept channels until the Session is closed and respond with the server handler in its own goroutine.
// If Handler was not set, an empty RespondMux is used. If the handler does not initiate a response, a nil value is
// returned. If the handler does not call Continue, the channel will be closed. Respond will panic if Codec is nil.
//
// If the context is not nil, it will be added to Calls. Otherwise the Call Context will be set to a context.Background().
func (s *Server) Respond(sess mux.Session, ctx context.Context) {
	defer sess.Close()

	if s.Codec == nil {
		panic("rpc.Respond: nil codec")
	}

	hn := s.Handler
	if hn == nil {
		hn = NewRespondMux()
	}

	for {
		ch, err := sess.Accept()
		if err != nil {
			if err == io.EOF {
				return
			}
			panic(err)
		}
		go s.respond(hn, sess, ch, ctx)
	}
}

func (s *Server) respond(hn Handler, sess mux.Session, ch mux.Channel, ctx context.Context) {
	framer := &FrameCodec{Codec: s.Codec}
	dec := framer.Decoder(ch)

	var call Call
	err := dec.Decode(&call)
	if err != nil {
		log.Println("rpc.Respond:", err)
		return
	}

	call.Selector = cleanSelector(call.Selector)
	call.Decoder = dec
	call.Caller = &Client{
		Session: sess,
		codec:   s.Codec,
	}
	if ctx == nil {
		call.Context = context.Background()
	} else {
		call.Context = ctx
	}
	call.ch = ch

	header := &ResponseHeader{}
	resp := &responder{
		ch:     ch,
		c:      framer,
		header: header,
	}

	hn.RespondRPC(resp, &call)
	if !resp.responded {
		resp.Return()
	}
	if !resp.header.Continue {
		ch.Close()
	}
}
func (s *Server) Call(ctx context.Context, selector string, args any, replies ...any) (*Response, error) {
	ch, err := s.sess.Open(ctx)
	if err != nil {
		return nil, err
	}
	// If the context is cancelled before the call completes, call Close() to
	// abort the current operation.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			ch.Close()
		case <-done:
		}
	}()
	resp, err := call(ctx, ch, s.Codec, selector, args, replies...)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return resp, ctxErr
	}
	return resp, err
}

// func call(ctx context.Context, ch mux.Channel, cd codec.Codec, selector string, args any, replies ...any) (*Response, error) {
// 	framer := &FrameCodec{Codec: cd}
// 	enc := framer.Encoder(ch)
// 	dec := framer.Decoder(ch)

// 	// request
// 	err := enc.Encode(CallHeader{
// 		Selector: selector,
// 	})
// 	if err != nil {
// 		ch.Close()
// 		return nil, err
// 	}

// 	argCh, isChan := args.(chan interface{})
// 	switch {
// 	case isChan:
// 		for arg := range argCh {
// 			if err := enc.Encode(arg); err != nil {
// 				ch.Close()
// 				return nil, err
// 			}
// 		}
// 	default:
// 		if err := enc.Encode(args); err != nil {
// 			ch.Close()
// 			return nil, err
// 		}
// 	}

// 	// response
// 	var header ResponseHeader
// 	err = dec.Decode(&header)
// 	if err != nil {
// 		ch.Close()
// 		return nil, err
// 	}

// 	if !header.Continue {
// 		defer ch.Close()
// 	}

// 	resp := &Response{
// 		ResponseHeader: header,
// 		Channel:        ch,
// 		codec:          framer,
// 	}
// 	if len(replies) == 1 {
// 		resp.Reply = replies[0]
// 	} else if len(replies) > 1 {
// 		resp.Reply = replies
// 	}
// 	if resp.Error != nil {
// 		return resp, RemoteError(*resp.Error)
// 	}

// 	if resp.Reply == nil {
// 		// read into throwaway buffer
// 		var buf []byte
// 		dec.Decode(&buf)
// 	} else {
// 		for _, r := range replies {
// 			if err := dec.Decode(r); err != nil {
// 				return resp, err
// 			}
// 		}
// 	}

// 	return resp, nil
// }
