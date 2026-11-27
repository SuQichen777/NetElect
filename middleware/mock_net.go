package middleware

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"raft-leader-election/server"
	"reflect"
	"strings"
	"sync"
	"time"
)

// MockNetwork simulates an in-process network.
// It invokes methods on the target object via reflection and uses gob encode/decode to mimic network transfer (a deep copy).
// Endpoints returned by Connect implement the server.Transport interface.
type MockNetwork struct {
	mu         sync.Mutex
	servers    map[string]*MockServer
	enabled    map[string]bool // whether each node is enabled (simulates crashes / partitions)
	links      map[string]map[string]bool
	reliable   bool // reliable mode (no loss, no delay)
	longDelays bool // whether long delays are enabled
}

type MockServer struct {
	svc     interface{}
	methods map[string]reflect.Method
}

// MakeMockNetwork creates a new mock network.
func MakeMockNetwork() *MockNetwork {
	rn := &MockNetwork{}
	rn.servers = map[string]*MockServer{}
	rn.enabled = map[string]bool{}
	rn.links = map[string]map[string]bool{}
	rn.reliable = true
	return rn
}

// Register adds a service object to the network under name (e.g. "1", "2", "3").
func (rn *MockNetwork) Register(name string, svc interface{}) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	rs := &MockServer{}
	rs.svc = svc
	rs.methods = map[string]reflect.Method{}

	// Collect all methods via reflection
	t := reflect.TypeOf(svc)
	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		rs.methods[method.Name] = method
	}

	rn.servers[name] = rs
	rn.enabled[name] = true
	if _, ok := rn.links[name]; !ok {
		rn.links[name] = map[string]bool{}
	}
}

// Enable enables or disables a node (simulates a partition or crash).
func (rn *MockNetwork) Enable(name string, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.enabled[name] = enabled
}

func (rn *MockNetwork) SetLink(from string, to string, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	m, ok := rn.links[from]
	if !ok {
		m = map[string]bool{}
		rn.links[from] = m
	}
	m[to] = enabled
}

// Reliable sets whether the network is reliable.
func (rn *MockNetwork) Reliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.reliable = yes
}

// LongDelays sets whether long delays are enabled.
func (rn *MockNetwork) LongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.longDelays = yes
}

// Connect creates a client endpoint connected to serverName.
// The returned object implements the server.Transport interface.
func (rn *MockNetwork) Connect(serverName string) server.Transport {
	return rn.ConnectFromTo("", serverName)
}

func (rn *MockNetwork) ConnectFromTo(from string, to string) server.Transport {
	return &MockClientEnd{
		net:      rn,
		fromName: from,
		toName:   to,
	}
}

// MockClientEnd is a client endpoint of MockNetwork; it implements server.Transport.
type MockClientEnd struct {
	net      *MockNetwork
	fromName string
	toName   string
}

// Call implements the server.Transport interface.
func (e *MockClientEnd) Call(method string, args interface{}, reply interface{}) bool {
	// 1. Network check and delay simulation
	e.net.mu.Lock()
	toName := e.toName
	server, ok := e.net.servers[toName]
	enabledTo := e.net.enabled[toName]
	reliable := e.net.reliable
	longDelays := e.net.longDelays
	fromName := e.fromName
	enabledFrom := true
	if fromName != "" {
		enabledFrom = e.net.enabled[fromName]
	}
	linkAllowed := true
	if fromName != "" {
		if m, ok := e.net.links[fromName]; ok {
			if allowed, ok := m[toName]; ok {
				linkAllowed = allowed
			}
		}
	}
	e.net.mu.Unlock()

	if !ok || !enabledTo || !enabledFrom || !linkAllowed {
		return false
	}

	// Unreliable network: random loss
	if !reliable {
		// short delay
		ms := rand.Intn(27)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// drop the message with 10% probability
		if rand.Intn(1000) < 100 {
			return false
		}
	}

	// Long delays (e.g. congestion, or delays before a partition heals)
	if longDelays {
		ms := rand.Intn(7000)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	// 2. Method lookup
	// method is usually of the form "Raft.RequestVote"; strip the prefix
	dot := strings.LastIndex(method, ".")
	methodName := method
	if dot >= 0 {
		methodName = method[dot+1:]
	}

	svcMethod, ok := server.methods[methodName]
	if !ok {
		fmt.Printf("MockNetwork: method %s not found on server %s\n", methodName, toName)
		return false
	}

	// 3. Serialize the arguments (deep copy)
	// gob mimics network transfer and keeps the callee from mutating the caller's objects through pointers
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(args); err != nil {
		fmt.Printf("MockNetwork: encode error for %s: %v\n", method, err)
		return false
	}

	// 4. Deserialize the arguments (server side)
	// Create a new object of the same type as args
	argsType := reflect.TypeOf(args)
	// If it is a pointer, take its element type
	if argsType.Kind() == reflect.Ptr {
		argsType = argsType.Elem()
	}
	// Create a new instance
	serverArgs := reflect.New(argsType).Interface()

	dec := gob.NewDecoder(&buf)
	if err := dec.Decode(serverArgs); err != nil {
		fmt.Printf("MockNetwork: decode error for %s: %v\n", method, err)
		return false
	}

	// 5. Prepare the reply object
	// reply is the caller's pointer; create a temporary reply on the server side, then encode it back
	replyType := reflect.TypeOf(reply)
	if replyType.Kind() != reflect.Ptr {
		fmt.Printf("MockNetwork: reply must be a pointer\n")
		return false
	}
	serverReply := reflect.New(replyType.Elem()).Interface()

	// 6. Reflective call
	// The signature is usually func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply)
	// Is the receiver (server.svc) already bound in svcMethod?
	// A reflect.Method obtained from a Type takes the receiver as the first argument of Func;
	// one obtained from a Value already has the receiver bound.
	// Here the reflect.Method comes from TypeOf, so the receiver must be passed explicitly.

	funcType := svcMethod.Type
	if funcType.NumIn() != 3 {
		fmt.Printf("MockNetwork: method %s has wrong number of args: %d\n", method, funcType.NumIn())
		return false
	}

	// Build the call arguments: [Receiver, Args, Reply]
	in := []reflect.Value{
		reflect.ValueOf(server.svc),
		reflect.ValueOf(serverArgs),
		reflect.ValueOf(serverReply),
	}

	// Execute the call
	e.net.mu.Lock()
	// Re-check that the node is still up, since it may have gone down during serialization
	enabledTo2 := e.net.enabled[toName]
	enabledFrom2 := true
	if fromName != "" {
		enabledFrom2 = e.net.enabled[fromName]
	}
	linkAllowed2 := true
	if fromName != "" {
		if m, ok := e.net.links[fromName]; ok {
			if allowed, ok := m[toName]; ok {
				linkAllowed2 = allowed
			}
		}
	}
	if !enabledTo2 || !enabledFrom2 || !linkAllowed2 {
		e.net.mu.Unlock()
		return false
	}
	e.net.mu.Unlock()

	// Invoke the service method
	// Note: panics are not recovered here; production code should recover
	svcMethod.Func.Call(in)

	// 7. Serialize the response
	var replyBuf bytes.Buffer
	replyEnc := gob.NewEncoder(&replyBuf)
	if err := replyEnc.Encode(serverReply); err != nil {
		fmt.Printf("MockNetwork: reply encode error for %s: %v\n", method, err)
		return false
	}

	// 8. Deserialize the response (back to the caller)
	replyDec := gob.NewDecoder(&replyBuf)
	if err := replyDec.Decode(reply); err != nil {
		fmt.Printf("MockNetwork: reply decode error for %s: %v\n", method, err)
		return false
	}

	return true
}
