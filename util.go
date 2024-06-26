package sharding

import (
	"errors"
	"log"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

func sessMustCreateWithData(
	sess *curator.Session, path string, flags int32, data []byte, callback func(resp zk.CreateResponse),
) {
	var loop func(sess *curator.Session)
	loop = func(sess *curator.Session) {
		sess.GetClient().Create(path, data, flags, func(resp zk.CreateResponse, err error) {
			if err == nil || errors.Is(err, zk.ErrNodeExists) {
				callback(resp)
				return
			}
			if errors.Is(err, zk.ErrConnectionClosed) {
				sess.AddRetry(loop)
				return
			}
			log.Panicf("Create node with error: %v, path: %s", err, path)
		})
	}
	loop(sess)
}

func sessMustCreatePersistence(
	sess *curator.Session, path string, callback func(resp zk.CreateResponse),
) {
	sessMustCreateWithData(sess, path, 0, nil, callback)
}

func sessMustChildren(sess *curator.Session, path string, callback func(resp zk.ChildrenResponse)) {
	var loop func(sess *curator.Session)
	loop = func(sess *curator.Session) {
		sess.GetClient().Children(path, func(resp zk.ChildrenResponse, err error) {
			if err != nil {
				if errors.Is(err, zk.ErrConnectionClosed) {
					sess.AddRetry(loop)
					return
				}
				panic(err)
			}
			callback(resp)
		})
	}
	loop(sess)
}
