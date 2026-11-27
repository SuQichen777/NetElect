package server

// Transport is a network abstraction that decouples the Raft logic from the underlying communication mechanism (mock, gRPC, or TCP).
//
// Why this interface?
// 1. Tests can run on the in-process mock network, which is fast and deterministic.
// 2. The same Raft core runs over gRPC or the custom TCP protocol without modification.
// 3. Fault injection is easy: a middleware layer can wrap it to add loss or delay.
type Transport interface {
	// Call sends an RPC request.
	// method: RPC method name, e.g. "Raft.RequestVote"
	// args: request arguments
	// reply: response (must be a pointer)
	// Returns true if the network exchange succeeded (a reply was received), false on timeout or network error.
	// Note: true does not mean the operation succeeded at the application level, only that the RPC itself completed.
	Call(method string, args interface{}, reply interface{}) bool
}

// Peer represents one peer node in the cluster.
type Peer struct {
	ID        int       // unique node identifier
	Transport Transport // client transport for reaching this node
}
