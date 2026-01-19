package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderIdx int
	clientId  int64
	requestId int64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, leaderIdx: 0}
	// You'll have to add code here.
	ck.clientId = time.Now().UnixNano()
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}

	for {
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[ck.leaderIdx], "KVServer.Get", args, &reply)

		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey
			case rpc.ErrWrongLeader:
				// 尝试下一个服务器
				ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			default:
				// 其他错误，尝试下一个服务器
				ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			}
		} else {
			// RPC 失败，尝试下一个服务器
			ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := &rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clientId, RequestId: ck.requestId}
	ck.requestId += 1
	firstAttempt := true

	for {
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[ck.leaderIdx], "KVServer.Put", args, &reply)

		if ok {
			switch reply.Err {
			case rpc.OK:
				return rpc.OK
			case rpc.ErrNoKey:
				return rpc.ErrNoKey
			case rpc.ErrVersion:
				if firstAttempt {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			case rpc.ErrWrongLeader:
				ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			default:
				ck.leaderIdx = (ck.leaderIdx + 1) % len(ck.servers)
			}
		} else {
			firstAttempt = false
		}

		time.Sleep(10 * time.Millisecond)
	}
}
