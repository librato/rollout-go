package rollout

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"log"

	"github.com/samuel/go-zookeeper/zk"
)

type Client interface {
	Start() error
	Stop()
	RawPercentage(feature string) (float64, error)
	FeatureActive(feature string, userId int64, userGroups []string) bool
}

// is called when a ZK error is encountered
type errorHandlerFunc func(err error)

type client struct {
	sync.RWMutex
	zk           *zk.Conn
	currentData  map[string]string
	stop         chan bool
	done         chan bool
	path         string
	errorHandler errorHandlerFunc
}

func NewClient(zk *zk.Conn, path string, errorHandler errorHandlerFunc) Client {
	return &client{
		zk:           zk,
		path:         path,
		currentData:  make(map[string]string),
		stop:         make(chan bool),
		done:         make(chan bool),
		errorHandler: errorHandler,
	}
}

func (r *client) Start() error {
	rolloutLog.Info("Starting Rollout service on ", r.path)
	exists, _, err := r.zk.Exists(r.path)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("Rollout path (%s) does not exist", r.path)
	}

	go r.poll(r.path)
	return nil
}

func (r *client) Stop() {
	r.stop <- true
	<-r.done
}

func (r *client) poll(path string) {
	defer func() { r.done <- true }()
	defer rolloutLog.Info("rollout poller shutdown")
	for {
		data, _, watch, err := r.zk.GetW(path)
		if err != nil {
			rolloutLog.Error("rollout failed to set watch: ", err)
			if r.errorHandler != nil {
				r.errorHandler(err)
			}

			select {
			case <-time.After(time.Second):
			case <-r.stop:
				return
			}
			continue
		}

		if err := r.swapData(data); err != nil {
			rolloutLog.Error("rollout couldn't unmarshal zookeeper data: ", err)
			// re-get the watch so we know when/if the bad data changes
			_, _, watch, err = r.zk.GetW(path)
			if err != nil {
				log.Fatal("could not re-establish a watch after unmarshalling error. Nothing to do but exit")
			}
		}

		select {
		case <-watch:
			// block until data changes
		case <-r.stop:
			return
		}
	}
}

func (r *client) swapData(data []byte) error {
	newMap := make(map[string]string)
	if err := json.Unmarshal(data, &newMap); err != nil {
		return err
	}

	r.Lock()
	defer r.Unlock()
	r.currentData = newMap
	return nil
}

// RawPercentage returns the raw percentage from the rollout section
func (r *client) RawPercentage(feature string) (float64, error) {
	feature = "feature:" + feature
	r.RLock()
	value, ok := r.currentData[feature]
	r.RUnlock()

	if !ok {
		return 0.0, fmt.Errorf("feature not found: %s", feature)
	}
	splitResult := strings.Split(value, "|")
	if len(splitResult) != 3 {
		return 0.0, fmt.Errorf("invalid value for %s: %s", feature, value)
	}
	percentageFloat, err := strconv.ParseFloat(splitResult[0], 64)
	if err != nil {
		return 0.0, fmt.Errorf("rollout invalid percentage: %v", splitResult[0])
	}
	return percentageFloat, nil
}

func (r *client) FeatureActive(feature string, userId int64, userGroups []string) bool {
	feature = "feature:" + feature
	r.RLock()
	value, ok := r.currentData[feature]
	r.RUnlock()

	if !ok {
		return false
	}
	splitResult := strings.Split(value, "|")
	if len(splitResult) != 3 {
		rolloutLog.Errorf("Rollout: invalid value for %s: %s", feature, value)
		return false
	}
	featureGroups := strings.Split(splitResult[2], ",")
	// Short-circuit for pseudo-group "all"
	if contains("all", featureGroups) {
		return true
	}
	percentageFloat, err := strconv.ParseFloat(splitResult[0], 64)
	if err != nil {
		rolloutLog.Error("rollout invalid percentage: ", splitResult[0])
		return false
	}
	percentage := int(percentageFloat)
	// Short-circuit for 100%
	if percentage == 100 {
		return true
	}
	// Check user ID
	userIds := strings.Split(splitResult[1], ",")
	userIdString := strconv.FormatInt(userId, 10)
	if contains(userIdString, userIds) {
		return true
	}

	// Next, check percentage
	if userId%100 < int64(percentage) {
		return true
	}

	// Lastly, check groups
	for _, userGroup := range userGroups {
		if contains(userGroup, featureGroups) {
			return true
		}
	}
	return false
}

func contains(needle string, haystack []string) bool {
	for _, i := range haystack {
		if i == needle {
			return true
		}
	}
	return false
}
