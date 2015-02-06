package rollout

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/librato/gozk"
)

type Client interface {
	Start() error
	Stop()
	FeatureActive(feature string, userId int64, userGroups []string) bool
}

// is called when a ZK error is encountered
type errorHandlerFunc func(err error)

type client struct {
	zk           *zookeeper.Conn
	currentData  map[string]string
	mutex        sync.RWMutex
	stop         chan bool
	done         chan bool
	path         string
	errorHandler errorHandlerFunc
}

func NewClient(zk *zookeeper.Conn, path string, errorHandler errorHandlerFunc) Client {
	r := client{
		zk:           zk,
		path:         path,
		currentData:  make(map[string]string),
		stop:         make(chan bool),
		done:         make(chan bool),
		errorHandler: errorHandler,
	}
	return &r
}

func (r *client) Start() error {
	log.Printf("Starting Rollout service on %s", r.path)
	stat, err := r.zk.Exists(r.path)
	if err != nil {
		return err
	}
	if stat == nil {
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
	for {
		data, _, watch, err := r.zk.GetW(path)
		if err != nil {
			log.Println("Rollout: Failed to set watch", err)
			if r.errorHandler != nil {
				r.errorHandler(err)
			}
			time.Sleep(time.Second)
			continue
		}
		newMap := make(map[string]string)
		err = json.Unmarshal([]byte(data), &newMap)
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

func (r *client) FeatureActive(feature string, userId int64, userGroups []string) bool {
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
	featureGroups := strings.Split(splitResult[2], ",")
	// Short-circuit for pseudo-group "all"
	if contains("all", featureGroups) {
		return true
	}
	percentage, err := strconv.Atoi(splitResult[0])
	if err != nil {
		log.Println("Rollout: Invalid percentage: ", splitResult[0])
		return false
	}
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
