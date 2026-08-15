package statemachine

// StatePrefix namespaces every key this state machine owns.
//
// The Raft log, current term, votedFor and lastApplied live in the same Pebble
// database (see example/db). Without a namespace of our own, a client SET could
// overwrite Raft metadata, and Snapshot would capture that metadata and ship it to
// another node — restoring one node's term and votedFor onto another breaks the
// safety property those fields exist to provide.
const StatePrefix = "sm:"

// stateKey namespaces a user-supplied key for storage.
func stateKey(key []byte) []byte {
	prefix := []byte(StatePrefix)
	k := make([]byte, len(prefix)+len(key))
	copy(k, prefix)
	copy(k[len(prefix):], key)
	return k
}

// userKey reverses stateKey, for reading a stored key back out. Callers iterating
// within the prefix bounds always have the prefix present.
func userKey(k []byte) []byte {
	return k[len(StatePrefix):]
}

// upperBound takes a key prefix and returns the smallest key greater than every key
// sharing that prefix, so an iterator can be bounded to exactly that namespace.
//
// Example: "sm:" → "sm;"
func upperBound(prefix []byte) []byte {
	upper := make([]byte, len(prefix))
	copy(upper, prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		upper[i]++
		if upper[i] != 0 {
			return upper[:i+1]
		}
	}
	// Every byte was 0xFF: no upper bound exists, iterate to the end.
	return nil
}
