package sharding

import (
	"errors"
	"log"

	"github.com/QuangTung97/zk"
	"github.com/QuangTung97/zk/curator"
)

func sessMustCreate(
	sess *curator.Session, path string, flags int32, callback func(resp zk.CreateResponse),
) {
	var loop func(sess *curator.Session)
	clientCall := func(client curator.Client) {
		client.Create(path, nil, flags, func(resp zk.CreateResponse, err error) {
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
	loop = func(sess *curator.Session) {
		sess.Run(clientCall)
	}
	loop(sess)
}

func sessMustCreatePersistence(
	sess *curator.Session, path string, callback func(resp zk.CreateResponse),
) {
	sessMustCreate(sess, path, 0, callback)
}

func sessMustChildren(sess *curator.Session, path string, callback func(resp zk.ChildrenResponse)) {
	var loop func(sess *curator.Session)
	clientCall := func(client curator.Client) {
		client.Children(path, func(resp zk.ChildrenResponse, err error) {
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
	loop = func(sess *curator.Session) {
		sess.Run(clientCall)
	}
	loop(sess)
}
