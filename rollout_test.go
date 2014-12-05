package rollout

import (
	"github.com/librato/gozk"
	"log"
	"testing"
	"time"
)

var (
	zk      = makeZK()
	path    = "/rollout-go-test"
	rollout = makeRollout(zk)
)

func TestUserId(t *testing.T) {
	setData(`{"feature:hello": "0|1|"}`)
	groups := []string{"foo"}
	assert(t, rollout.FeatureActive("hello", 1, groups), "feature should be active")
	assert(t, !rollout.FeatureActive("hello", 2, groups), "feature should not be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groups), "feature should not be active")
}

func TestGroup(t *testing.T) {
	setData(`{"feature:hello": "0||foo"}`)
	groupA := []string{"foo"}
	groupB := []string{"bar"}
	assert(t, rollout.FeatureActive("hello", 1, groupA), "feature should be active")
	assert(t, !rollout.FeatureActive("hello", 2, groupB), "feature should not be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groupA), "feature should not be active")
}

func TestAll(t *testing.T) {
	setData(`{"feature:hello": "0||all"}`)
	group := []string{""}
	assert(t, rollout.FeatureActive("hello", 1, group), "feature should be active")
	assert(t, rollout.FeatureActive("hello", 2, group), "feature should be active")
}

func TestPercentage(t *testing.T) {
	groups := []string{"foo"}
	setData(`{"feature:hello": "0||"}`)
	assert(t, !rollout.FeatureActive("hello", 1, groups), "feature should not be active")
	assert(t, !rollout.FeatureActive("hello", 2, groups), "feature should not be active")
	setData(`{"feature:hello": "25||"}`)
	assert(t, rollout.FeatureActive("hello", 1, groups), "feature should be active")
	assert(t, !rollout.FeatureActive("hello", 26, groups), "feature should not be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groups), "feature should not be active")
	setData(`{"feature:hello": "50||"}`)
	assert(t, rollout.FeatureActive("hello", 1, groups), "feature should be active")
	assert(t, rollout.FeatureActive("hello", 26, groups), "feature should be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groups), "feature should not be active")
}

func makeZK() *zookeeper.Conn {
	zk, session, err := zookeeper.Dial("localhost:2181", 5e9)
	if err != nil {
		log.Fatal(err)
	}
	event := <-session
	if event.State != zookeeper.STATE_CONNECTED {
		log.Fatal("Cannot initialize zookeeper: ", event.State)
	}
	return zk
}

func makeRollout(zk *zookeeper.Conn) Client {
	rollout := NewClient(zk, path)
	// 1 == ephemeral
	zk.Create(path, "{}", 1, zookeeper.WorldACL(zookeeper.PERM_ALL))
	err := rollout.Start()
	if err != nil {
		log.Fatal(err)
	}
	return rollout
}

func setData(data string) error {
	_, err := zk.Set(path, data, -1)
	time.Sleep(100 * time.Millisecond)
	return err
}

func assert(t *testing.T, condition bool, explanation interface{}) {
	if !condition {
		t.Error(explanation)
		t.FailNow()
	}
}
