package rollout

import (
	"encoding/json"
	"fmt"
	"github.com/librato/gozk"
	"log"
	"strconv"
	"strings"
	"sync"
)

type Rollout struct {
	zk          *zookeeper.Conn
	currentData map[string]string
	mutex       sync.RWMutex
	stop        chan bool
	done        chan bool
}

func (r *Rollout) Start(path string) error {
	log.Printf("Starting Rollout service on %s", path)
	stat, err := r.zk.Exists(path)
	if err != nil {
		return err
	}
	if stat == nil {
		return fmt.Errorf("Rollout path (%s) does not exist", path)
	}

	go r.poll(path)
	return nil
}

func (r *Rollout) Stop() {
	r.stop <- true
	<-r.done
}

func (r *Rollout) poll(path string) {
	defer func() { r.done <- true }()
	for {
		data, _, watch, err := r.zk.GetW(path)
		if err != nil {
			log.Println("Rollout: Failed to set watch", err)
			continue
		}
		newMap := make(map[string]string)
		err = json.Unmarshal([]byte(data), newMap)
		if err != nil {
			log.Println("Rollout: Couldn't unmarshal zookeeper data", err)
		}
		r.mutex.Lock()
		r.currentData = newMap
		r.mutex.Unlock()
		select {
		case <-watch:
			// block until data changes
		case <-r.stop:
			log.Println("Rollout poller shutdown")
			return
		}
	}
}

func (r *Rollout) FeatureActive(feature string, userId int64, userGroups []string) bool {
	feature = "feature:" + feature
	r.mutex.RLock()
	value, ok := r.currentData[feature]
	r.mutex.RUnlock()
	if !ok {
		return false
	}
	splitResult := strings.Split(value, "|")
	if len(splitResult) != 3 {
		log.Println("Rollout: invalid value for ", feature, ":", value)
		return false
	}
	// Check user ID first
	userIds := strings.Split(splitResult[1], ",")
	userIdString := string(userId)
	if contains(userIdString, userIds) {
		return true
	}

	// Next, check percentage
	percentage, err := strconv.Atoi(splitResult[0])
	if err != nil {
		log.Println("Rollout: Invalid percentage: ", splitResult[0])
		return false
	}
	if userId%10 < int64(percentage/10) {
		return true
	}

	// Lastly, check groups
	featureGroups := strings.Split(splitResult[2], ",")
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
