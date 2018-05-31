package rollout

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

type ZookeeperClient struct {
	sync.Mutex
	*zk.Conn
	Errors chan error
	Hosts  []string
}

func NewZookeeperClient(hostString string) *ZookeeperClient {
	log.Info("creating a new Zookeeper client with hosts ", hostString)
	return &ZookeeperClient{
		Errors: make(chan error),
		Hosts:  strings.Split(hostString, ","),
	}
}

func (zkc *ZookeeperClient) Start() {
	log.Info("starting the zookeeper client ")

	conn, eventChan, err := zk.Connect(zkc.Hosts, 30*time.Second)
	if err != nil {
		log.Fatal("err while connecting to zookeeper: ", err)
	}

	event := <-eventChan
	if event.Type != zk.EventSession {
		log.Fatal("cannot initialize zookeeper. state: ", event.State)
	}

	log.Info("connected to Zookeeper")
	zkc.Conn = conn

	go func() {
		for {
			for event := range eventChan {
				log.Debug("Got event ", event)
				if event.Type == zk.EventSession {
					switch event.State {
					case zk.StateUnknown, zk.StateExpired:
						log.Error("fatal zookeeper event, shutting down. state:  ", event.State)
						zkc.Errors <- fmt.Errorf("got non-recoverable event: %+v", event)
					case zk.StateConnected, zk.StateHasSession:
					default:
						log.Info("received unknown ZK event ", event)
					}
				}
			}

		}
	}()
}

// Creates a znode at the given string, including any intervening, required paths
func (zkc *ZookeeperClient) CreateFullNode(node string) error {
	if strings.HasPrefix(node, "/") == false {
		return errors.New("specify the full path to the new node")
	}

	log.Info("creating ZK node " + node)

	currpath := ""
	for _, pathseg := range strings.Split(node, "/") {
		if pathseg == "" {
			// skip badly formed paths with two slashes
			continue
		}

		currpath = currpath + "/" + pathseg
		log.Info("creating zk node " + currpath)
		if exists, _, err := zkc.Exists(currpath); err != nil {
			return err
		} else if exists {
			continue
		}

		if _, err := zkc.Create(currpath, []byte(""), 0, zk.WorldACL(zk.PermAll)); err != nil {
			return err
		}

		if exists, _, err := zkc.Exists(currpath); err != nil {
			return err
		} else if exists == false {
			return errors.New("failed to create a node on the ZK host! " + currpath)
		}
	}

	return nil
}

// A WatchedNode associates a znode with a function that unmarshals it and its latest
// node version.
type WatchedNode struct {
	Unmarshal   func([]byte) error
	Marshal     func([]byte, interface{}) error
	zNode       string
	nodeVersion int32
}

// A WatchingClient maintains a watch on a specific znode, calling a provided unmarshalling
// func when its updated. This unmarshalling func is executed within a critical section and
// should not take a long time to execute. A setter for the watched node is provided, which
// has the advantage of keeping track of the node's version for you. For better or worse,
// this client creates one polling gofunc per watched node.
type WatchingClient struct {
	sync.Mutex
	*ZookeeperClient
	*WatchedNode

	ErrorHandler func(error)
	Done         chan bool
}

// Creates a new WatchingClient for the node at the given string
func NewWatchingClient(
	zkClient *ZookeeperClient,
	zNode string,
	marshaller func([]byte, interface{}) error,
	unmarshaller func([]byte) error,
	errorHandler func(error)) *WatchingClient {

	wNode := &WatchedNode{
		Marshal:     marshaller,
		Unmarshal:   unmarshaller,
		zNode:       zNode,
		nodeVersion: 0,
	}

	return &WatchingClient{
		ZookeeperClient: zkClient,
		WatchedNode:     wNode,
		ErrorHandler:    errorHandler,
		Done:            make(chan bool),
	}
}

// Checks that the watched znode exists in zookeeper (returning an error if it's not), then
// executes a gofunc that updates the local data from the bytes in the znode when its data
// changes.
func (wc *WatchingClient) Start() error {
	log.Info("Starting Rollout service on %s", wc.zNode)
	exists, _, err := wc.Exists(wc.zNode)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("Rollout path (%s) does not exist", wc.zNode)
	}

	go wc.poll()
	return nil
}

func (wc *WatchingClient) Stop() {
	wc.Done <- true
}

// Writes the given thing to the znode, calling the marshalling func to go from thing to
// bytes. This func only writes to the znode, and local vars are not updated -- they will
// be updated by the polling loop.
func (wc *WatchingClient) Write(thing interface{}) error {
	bytes := make([]byte, 0)
	if err := wc.Marshal(bytes, thing); err != nil {
		log.Error("Could not marshal a thing to znode %v: %v\n", wc.zNode, err)
		return err
	}

	// don't set our local vars -- it'll be updated in poll()
	if _, err := wc.Set(wc.zNode, bytes, wc.nodeVersion); err != nil {
		log.Error("Could not set bytes to znode %v: %v\n", wc.zNode, err)
		return err
	}

	return nil
}

func (wc *WatchingClient) poll() {
	defer log.Info("exiting from WatchingClient polling loop")

	for {
		data, stat, watch, err := wc.ZookeeperClient.GetW(wc.zNode)
		if err != nil {
			log.Error("failed to get data/set watch: %v\n", err)
			if wc.ErrorHandler != nil {
				wc.ErrorHandler(err)
			}
			select {
			case <-time.After(time.Second):
			case <-wc.Done:
				return
			}
			continue
		}

		wc.Lock()
		if err := wc.Unmarshal(data); err != nil {
			log.Error("error while unmarshalling: %v -- rewatching node, leaving data unchanged\n", err)
			// re-get the watch so we know when/if the bad data changes
			_, _, watch, err = wc.ZookeeperClient.GetW(wc.zNode)
			if err != nil {
				log.Fatal("could not re-establish a watch after unmarshalling error. Nothing to do but exit")
			}
		}
		wc.nodeVersion = stat.Version
		wc.Unlock()

		select {
		case <-watch:
			log.Debug("watch triggered, rereading system messages node %s\n", wc.zNode)
		case <-wc.Done:
			return
		}
	}
}
