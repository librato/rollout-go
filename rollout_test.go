package rollout

import (
	"testing"
)

func TestUserId(t *testing.T) {
	rollout := &client{currentData: make(map[string]string)}
	rollout.swapData([]byte(`{"feature:hello": "0|1|"}`))
	groups := []string{"foo"}
	assert(t, rollout.FeatureActive("hello", 1, groups), "feature should be active")
	assert(t, !rollout.FeatureActive("hello", 2, groups), "feature should not be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groups), "feature should not be active")
}

func TestGroup(t *testing.T) {
	rollout := &client{currentData: make(map[string]string)}
	rollout.swapData([]byte(`{"feature:hello": "0||foo"}`))
	groupA := []string{"foo"}
	groupB := []string{"bar"}
	assert(t, rollout.FeatureActive("hello", 1, groupA), "feature should be active")
	assert(t, !rollout.FeatureActive("hello", 1, groupB), "feature should not be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groupA), "feature should not be active")
}

func TestAll(t *testing.T) {
	rollout := &client{currentData: make(map[string]string)}
	rollout.swapData([]byte(`{"feature:hello": "0||all"}`))
	group := []string{""}
	assert(t, rollout.FeatureActive("hello", 1, group), "feature should be active")
	assert(t, rollout.FeatureActive("hello", 2, group), "feature should be active")
}

func TestPercentage(t *testing.T) {
	rollout := &client{currentData: make(map[string]string)}
	groups := []string{"foo"}
	rollout.swapData([]byte(`{"feature:hello": "0.0||"}`))
	assert(t, !rollout.FeatureActive("hello", 1, groups), "feature should not be active")
	assert(t, !rollout.FeatureActive("hello", 2, groups), "feature should not be active")
	rollout.swapData([]byte(`{"feature:hello": "25.0||"}`))
	assert(t, rollout.FeatureActive("hello", 1, groups), "feature should be active")
	assert(t, !rollout.FeatureActive("hello", 26, groups), "feature should not be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groups), "feature should not be active")
	rollout.swapData([]byte(`{"feature:hello": "50||"}`))
	assert(t, rollout.FeatureActive("hello", 1, groups), "feature should be active")
	assert(t, rollout.FeatureActive("hello", 26, groups), "feature should be active")
	assert(t, !rollout.FeatureActive("nosuchfeature", 1, groups), "feature should not be active")
}

func assert(t *testing.T, condition bool, explanation interface{}) {
	if !condition {
		t.Error(explanation)
		t.FailNow()
	}
}
