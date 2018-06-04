// +build integration

package rollout

import "testing"

const (
	TestPath = "/put/tests/here"
)

func TestZookeeperClient(t *testing.T) {
	zk := NewZookeeperClient("localhost:2181")
	zk.Start()

	zk.Delete(TestPath, -1)

	before, _, err := zk.Exists(TestPath)
	assert(t, err == nil, "err calling 'before' exists")
	assert(t, ! before, "test path exists")

	zk.CreateFullNode(TestPath, true)

	after, _, err := zk.Exists(TestPath)
	assert(t, err == nil, "err calling 'after' exists")
	assert(t, after, "test path does not exists")

	zk.Delete(TestPath, -1)
}
