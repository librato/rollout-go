package rollout

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

type ZookeeperClient struct {
	*zk.Conn
	Errors chan error
	Hosts  []string
}

func NewZookeeperClient(hostString string) *ZookeeperClient {
	rolloutLog.Info("creating a new Zookeeper client with hosts ", hostString)
	return &ZookeeperClient{
		Errors: make(chan error),
		Hosts:  strings.Split(hostString, ","),
	}
}

func (zkc *ZookeeperClient) Start() {
	rolloutLog.Info("starting the zookeeper client")

	conn, eventChan, err := zk.Connect(zkc.Hosts, 30*time.Second)
	if err != nil {
		rolloutLog.Fatal("err while connecting to zookeeper: ", err)
	}

	event := <-eventChan
	if event.Type != zk.EventSession {
		rolloutLog.Fatal("cannot initialize zookeeper. state: ", event.State)
	}

	rolloutLog.Info("connected to Zookeeper")
	zkc.Conn = conn

	go func() {
		for {
			for event := range eventChan {
				rolloutLog.Debug("got event ", event)
				if event.Type == zk.EventSession {
					switch event.State {
					case zk.StateUnknown, zk.StateExpired:
						rolloutLog.Error("fatal zookeeper event, shutting down. state: ", event.State)
						zkc.Errors <- fmt.Errorf("got non-recoverable event: %+v", event)
					case zk.StateConnected, zk.StateHasSession:
					default:
						rolloutLog.Info("received unknown ZK event ", event)
					}
				}
			}

		}
	}()
}

// Creates a znode at the given string, including any intervening, required paths
func (zkc *ZookeeperClient) CreateFullNode(node string, ephemeral bool) error {
	if strings.HasPrefix(node, "/") == false {
		return errors.New("specify the full path to the new node")
	}

	rolloutLog.Info("creating ZK node ", node)

	var flags int32 = 0
	if ephemeral {
		flags = 1
	}

	currpath := ""
	for _, pathseg := range strings.Split(node, "/") {
		if pathseg == "" {
			// skip badly formed paths with two slashes
			continue
		}

		currpath = currpath + "/" + pathseg
		rolloutLog.Debug("creating zk node ", currpath)
		if exists, _, err := zkc.Exists(currpath); err != nil {
			return err
		} else if exists {
			continue
		}

		if _, err := zkc.Create(currpath, []byte(""), flags, zk.WorldACL(zk.PermAll)); err != nil {
			return err
		}

		if exists, _, err := zkc.Exists(currpath); err != nil {
			return err
		} else if exists == false {
			return errors.New("failed to create a node on the ZK host " + currpath)
		}
	}

	return nil
}
