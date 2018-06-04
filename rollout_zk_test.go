// +build integration

package rollout

import (
	"testing"
	"time"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	path = "/rollout-go-test"
)

func makeZK() *zk.Conn {
	zookeeper, session, err := zk.Connect([]string{"localhost:2181"}, 5e9)
	if err != nil {
		log.Fatal(err)
	}
	event := <-session
	if event.Type != zk.EventSession {
		log.Fatal("Cannot initialize zookeeper: ", event.State)
	}
	return zookeeper
}

func TestClient_FeatureActive(t *testing.T) {
	zookeeper := makeZK()

	rollout := NewClient(zookeeper, path, nil)
	// 1 == ephemeral

	err := rollout.Start()
	assert(t, err != nil, "start should err because the rollout node doesn't exist")

	zookeeper.Create(path, []byte("{}"), 1, zk.WorldACL(zk.PermAll))
	if err := rollout.Start(); err != nil {
		log.Fatal(err)
	}

	if _, err := zookeeper.Set(path, []byte(`{"feature:hello": "0|1|"}`), -1); err != nil {
		log.Fatal(err)
	}

	time.Sleep(250 * time.Millisecond)

	groups := []string{"foo"}
	assert(t, rollout.FeatureActive("hello", 1, groups), "feature should be active")
}
