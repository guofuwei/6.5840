package rpc

type Err string

const (
	// Err's returned by server and Clerk
	OK         = "OK"
	ErrNoKey   = "ErrNoKey"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
	// ErrStaleConfig is returned by shard migration RPCs only when the
	// request's configuration number is older than the shard's number.
	ErrStaleConfig = "ErrStaleConfig"
)

type Tversion uint64

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion

	// for kvraft lab
	// ClientId is an unique identifier for each client
	ClientId int64
	// RequestId is a monotonically increasing number for each request from a client
	RequestId int64
}

type PutReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Err     Err
}
