package fn

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/roachadam/qtalk-go/rpc"
)

// HandlerFrom uses reflection to return a handler from either a function or
// methods from a struct. When a struct is used, HandlerFrom creates a RespondMux
// registering each method as a handler using its method name. From there, methods
// are treated just like functions.
//
// The registered methods can be limited by providing an interface type parameter:
//
//	h := HandlerFrom[interface{
//		OnlyTheseMethods()
//		WillBeRegistered()
//	}](myHandlerImplementation)
//
// If a struct method matches the HandlerFunc signature, the method will be called
// directly with the handler arguments. Otherwise it will be wrapped as described below.
//
// Function handlers expect an array to use as arguments. If the incoming argument
// array is too large or too small, the handler returns an error. Functions can opt-in
// to take a final Call pointer argument, allowing the handler to give it the Call value
// being processed. Functions can return nothing which the handler returns as nil, or
// a single value which can be an error, or two values where one value is an error.
// In the latter case, the value is returned if the error is nil, otherwise just the
// error is returned. Handlers based on functions that return more than two values will
// simply ignore the remaining values.
//
// Structs that implement the Handler interface will be added as a catch-all handler
// along with their individual methods. This lets you implement dynamic methods.
func HandlerFrom[T any](v T) rpc.Handler {
	rv := reflect.Indirect(reflect.ValueOf(v))
	switch rv.Type().Kind() {
	case reflect.Func:
		return fromFunc(reflect.ValueOf(v))
	case reflect.Struct:
		t := reflect.TypeOf((*T)(nil)).Elem()
		return fromMethods(v, t)
	default:
		panic("must be func or struct")
	}
}

// Args is the expected argument value for calls made to HandlerFrom handlers.
// Since it is just a slice of empty interface values, you can alternatively use
// more specific slice types ([]int{}, etc) if all arguments are of the same type.
type Args []any

var handlerFuncType = reflect.TypeOf((*rpc.HandlerFunc)(nil)).Elem()

func fromMethods(rcvr interface{}, t reflect.Type) rpc.Handler {
	// If `t` is an interface, `Convert()` wraps the value with that interface
	// type. This makes sure that the Method(i) indexes match for getting both the
	// name and implementation.
	rcvrval := reflect.ValueOf(rcvr).Convert(t)
	mux := rpc.NewRespondMux()
	for i := 0; i < t.NumMethod(); i++ {
		m := rcvrval.Method(i)
		var h rpc.Handler
		if m.CanConvert(handlerFuncType) {
			h = m.Convert(handlerFuncType).Interface().(rpc.HandlerFunc)
		} else {
			h = fromFunc(m)
		}
		mux.Handle(t.Method(i).Name, h)
	}
	h, ok := rcvr.(rpc.Handler)
	if ok {
		mux.Handle("/", h)
	}
	return mux
}

var callRef = reflect.TypeOf((*rpc.Call)(nil))

func fromFunc(fn reflect.Value) rpc.Handler {
	fntyp := fn.Type()
	// if the last argument in fn is an rpc.Call, add our call to fnParams
	expectsCallParam := fntyp.NumIn() > 0 && fntyp.In(fntyp.NumIn()-1) == callRef

	return rpc.HandlerFunc(func(r rpc.Responder, c *rpc.Call) {
		defer func() {
			if p := recover(); p != nil {
				r.Return(fmt.Errorf("panic: %s [%s]", p, identifyPanic()))
			}
		}()

		var params []any
		if err := c.Receive(&params); err != nil {
			r.Return(fmt.Errorf("fn: args: %s", err.Error()))
			return
		}
		if expectsCallParam {
			params = append(params, c)
		}
		ret, err := Call(fn.Interface(), params)
		if err != nil {
			r.Return(err)
			return
		}
		r.Return(ret...)
	})
}

// ensureType ensures a value is converted to the expected
// defined type from a convertable underlying type
func ensureType(v reflect.Value, t reflect.Type) reflect.Value {
	nv := v
	if v.Type().Kind() == reflect.Slice && v.Type().Elem() != t {
		switch t.Kind() {
		case reflect.Array:
			nv = reflect.Indirect(reflect.New(t))
			for i := 0; i < v.Len(); i++ {
				vv := reflect.ValueOf(v.Index(i).Interface())
				nv.Index(i).Set(vv.Convert(nv.Type().Elem()))
			}
		case reflect.Slice:
			nv = reflect.MakeSlice(t, 0, 0)
			for i := 0; i < v.Len(); i++ {
				vv := reflect.ValueOf(v.Index(i).Interface())
				nv = reflect.Append(nv, vv.Convert(nv.Type().Elem()))
			}
		default:
			panic("unable to convert slice to non-array, non-slice type")
		}
	}
	if v.Type() != t {
		nv = nv.Convert(t)
	}
	return nv
}

func identifyPanic() string {
	var name, file string
	var line int
	var pc [16]uintptr

	n := runtime.Callers(3, pc[:])
	for _, pc := range pc[:n] {
		fn := runtime.FuncForPC(pc)
		if fn == nil {
			continue
		}
		file, line = fn.FileLine(pc)
		name = fn.Name()
		if !strings.HasPrefix(name, "runtime.") {
			break
		}
	}

	switch {
	case name != "":
		return fmt.Sprintf("%v:%v", name, line)
	case file != "":
		return fmt.Sprintf("%v:%v", file, line)
	}

	return fmt.Sprintf("pc:%x", pc)
}
