package rollout

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

type ZookeeperClient struct {
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
	log.Info("starting the zookeeper client")

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
						log.Error("fatal zookeeper event, shutting down. state: ", event.State)
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

	log.Info("creating ZK node ", node)

	currpath := ""
	for _, pathseg := range strings.Split(node, "/") {
		if pathseg == "" {
			// skip badly formed paths with two slashes
			continue
		}

		currpath = currpath + "/" + pathseg
		log.Info("creating zk node ", currpath)
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
			return errors.New("failed to create a node on the ZK host " + currpath)
		}
	}

	return nil
}
