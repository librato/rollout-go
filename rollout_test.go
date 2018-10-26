package rollout

import (
	"fmt"
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

func TestRawPercentage(t *testing.T) {
	rollout := &client{currentData: make(map[string]string)}
	t.Run("happy path", func(t *testing.T) {
		rollout.swapData([]byte(`{"feature:apm_sample_rate_factor_org123456": "75.0||"}`))
		r, e := rollout.RawPercentage("apm_sample_rate_factor_org123456")
		assert(t, r == 75.0, "rawpercentage happy path test b0rked")
		assert(t, e == nil, "rawpercentage happy path test got non-nil error")
	})
	t.Run("missing org", func(t *testing.T) {
		rollout.swapData([]byte(`{"feature:apm_sample_rate_factor_org123456": "75.0||"}`))
		r, e := rollout.RawPercentage("apm_sample_rate_factor_org111111")
		assert(t, r == 0.0, "rawpercentage missing org test b0rked")
		assert(t, e != nil, "rawpercentage missing org test got nil error")
		assert(t,
			e.Error() == "feature not found: feature:apm_sample_rate_factor_org111111",
			fmt.Sprintf("rawpercentage missing org test got wrong error: %v", e.Error()))
	})
	t.Run("bad data (splits)", func(t *testing.T) {
		rollout.swapData([]byte(`{"feature:apm_sample_rate_factor_org123456": "75.0|"}`))
		r, e := rollout.RawPercentage("apm_sample_rate_factor_org123456")
		assert(t, r == 0.0, "rawpercentage bad data (splits) test b0rked")
		assert(t, e != nil, "rawpercentage bad data (splits) test got nil error")
		assert(t,
			e.Error() == "invalid value for feature:apm_sample_rate_factor_org123456: 75.0|",
			fmt.Sprintf("rawpercentage missing org test got wrong error: %v", e.Error()))
	})
	t.Run("bad data (values)", func(t *testing.T) {
		rollout.swapData([]byte(`{"feature:apm_sample_rate_factor_org123456": "dorkfish||"}`))
		r, e := rollout.RawPercentage("apm_sample_rate_factor_org123456")
		assert(t, r == 0.0, "rawpercentage bad data (values) test b0rked")
		assert(t, e != nil, "rawpercentage bad data (values) test got nil error")
		assert(t,
			e.Error() == "rollout invalid percentage: dorkfish",
			fmt.Sprintf("rawpercentage missing org test got wrong error: %v", e.Error()))
	})
}

func assert(t *testing.T, condition bool, explanation interface{}) {
	if !condition {
		t.Error(explanation)
		t.FailNow()
	}
}
