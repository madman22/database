package database2

import (
	//"bytes"
	"encoding/gob"
	"net"

	//"bytes"
	"context"
	//"errors"
	"strconv"
	"testing"
	"time"

	"github.com/twinj/uuid"
)

func TestBadgerDatabase(t *testing.T) {
	db, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	var ts string
	if err := db.Get("Test2", &ts); err != nil {
		t.Log(err.Error())
	} else {
		t.Log("got Test2:" + ts)
	}
	if err := db.Set("Test", "Value"); err != nil {
		t.Log("error setting Test:", err.Error())
	} else {
		var out string
		if err := db.Get("Test", &out); err != nil {
			t.Error(err.Error())
		}
		t.Log("got Test:", out)
	}
	if err := db.Set("Test", "Update"); err != nil {
		t.Log(err.Error())
	} else {
		var out string
		if err := db.Get("Test", &out); err != nil {
			t.Error(err.Error())
		}
		t.Log("got update Test:", out)
		if err := db.Delete("Test"); err != nil {
			t.Log(err.Error())
		}
	}
	var bad string
	if err := db.Get("Missing", &bad); err != nil {
		t.Log(err.Error())
	}
	if err := db.Set("Test2", "Value3"); err != nil {
		t.Log(err.Error())
	}

	if err := db.Close(); err != nil {
		t.Log(err.Error())
	}
}

func TestBadgerSetValue(t *testing.T) {
	ddb, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if err := ddb.SetValue("TEST1", []byte("TEST1")); err != nil {
		t.Error(err.Error())
		return
	}
	//var content []byte
	content, err := ddb.GetValue("TEST1")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if string(content) != "TEST1" {
		t.Error("TEST1 output does not equal input")
		return
	}

	ndb, err := ddb.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := ndb.SetValue("TEST2", []byte("TEST2")); err != nil {
		t.Error(err.Error())
		return
	}
	//var content2 []byte
	content2, err := ndb.GetValue("TEST2")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if string(content2) != "TEST2" {
		t.Error("TEST2 output does not equal input", string(content2))
		return
	}

	if err := ddb.Close(); err != nil {
		t.Log(err.Error())
	}
}

func TestBadgerDatabaseNode(t *testing.T) {
	ddb, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	db, err := ddb.NewNode("TestNode")
	if err != nil {
		t.Error(err.Error())
		return
	}
	var ts string
	if err := db.Get("Test2", &ts); err != nil {
		t.Log(err.Error())
	} else {
		t.Log("got Test2:" + ts)
	}
	if err := db.Set("Test", "Value"); err != nil {
		t.Log("error setting Test:", err.Error())
	} else {
		var out string
		if err := db.Get("Test", &out); err != nil {
			t.Error(err.Error())
		}
		t.Log("got Test:", out)
	}
	if err := db.Set("Test", "Update"); err != nil {
		t.Log(err.Error())
	} else {
		var out string
		if err := db.Get("Test", &out); err != nil {
			t.Error(err.Error())
		}
		t.Log("got update Test:", out)
		if err := db.Delete("Test"); err != nil {
			t.Log(err.Error())
		}
	}
	var bad string
	if err := db.Get("Missing", &bad); err != nil {
		t.Log(err.Error())
	}
	if err := db.Set("Test2", "Value3"); err != nil {
		t.Log(err.Error())
	}

	//node, err := db.NewNode("SecondNode")
	//testSetDB(t, node, 100)

	if err := ddb.Close(); err != nil {
		t.Log(err.Error())
	}
}

type Device struct {
	ID          string
	IP          net.IP
	Macs        []net.HardwareAddr
	Description string
	Username    string
	Password    string
}

func TestDatabaseDeep(t *testing.T) {
	gob.Register(&net.IP{})
	gob.Register(&net.HardwareAddr{})
	gob.Register(&[]net.HardwareAddr{})
	//db, err := NewBadger("testingdb/testingDeep", context.Background(), 2*time.Second)
	//db, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	t.Log(("Starting DB Test"))
	//db, err := NewORMDatabaseMemory("testingdb/testingDeep")
	db, err := NewInMemoryBadger(context.Background(), 1*time.Minute)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer db.Close()
	t.Log("Created new badger database")

	var dev Device
	dev.ID = uuid.NewV4().String()
	dev.IP = net.ParseIP("192.168.88.1")
	mac, err := net.ParseMAC("64:d1:54:b5:11:91")
	if err != nil {
		t.Error(err.Error())
		return
	}
	dev.Username = "aspen"
	dev.Macs = []net.HardwareAddr{mac}
	dev.Description = "This is a test device!"
	if err := db.Set(dev.ID, &dev); err != nil {
		t.Error(err.Error())
	} else {
		t.Log("Wrote Device!")
	}

	for x := 0; x <= 10; x++ {
		if err := db.Set("Test "+strconv.Itoa(x), strconv.Itoa(x)); err != nil {
			t.Log(err.Error())
		}
	}
	t.Log("Wrote 10 elements to root db")
	for x := 1; x <= 10; x++ {
		//node, err := db.NewNode("Node " + strconv.Itoa(x))
		//node, err := db.NewORMNode("Node " + strconv.Itoa(x))
		node, err := db.NewNode("Node " + strconv.Itoa(x))
		if err != nil {
			t.Log(err.Error())
			continue
		}
		if err := node.Set("Test"+strconv.Itoa(x), x); err != nil {
			t.Log(err.Error())
		}
		if x%2 != 0 {
			for y := 1; y <= x; y++ {
				//if dn, err := node.NewNode("Deep " + strconv.Itoa(x) + " " + strconv.Itoa(y)); err != nil {
				if dn, err := node.NewNode("Deep " + strconv.Itoa(x) + " " + strconv.Itoa(y)); err != nil {
					t.Log(err.Error())
				} else {
					if err := dn.Set("Test "+strconv.Itoa(x), strconv.Itoa(x+y)); err != nil {
						t.Log(err.Error())
					}
					if y%2 != 0 {
						//nn, err := dn.NewNode("Even Deeper!")
						nn, err := dn.NewNode("Even Deeper!")
						if err != nil {
							t.Log(err.Error())
						} else {
							for w := 1; w < 10; w++ {
								if err := nn.Set("Deeper", w); err != nil {
									t.Log(err.Error())
								}
							}
						}
					}
				}
			}
		}
	}

	nodes, err := db.GetNodes()
	if err != nil {
		t.Log(err.Error())
	} else {
		t.Log("Got nodes:", nodes)
		for _, node := range nodes {
			nd, err := db.NewNode(node)
			if err != nil {
				t.Log(err.Error())
				continue
			}
			var body string
			if vals, err := nd.GetAll(); err != nil {
				t.Log(err.Error())
			} else {
				body += "Items:" + strconv.Itoa(len(vals))
			}
			if nl, err := nd.GetNodes(); err != nil {
				t.Log(err.Error())
			} else {
				body += "Nodes:" + strconv.Itoa(len(nl))
			}
			t.Log("Node:" + node + " " + body)
		}
	}

	if n7, err := db.NewNode("Node 7"); err != nil {
		t.Log(err.Error())
	} else {
		if items, err := n7.GetAll(); err != nil {
			t.Log(err.Error())
		} else {
			if len(items) > 0 {
				for id, val := range items {
					//dec := gob.NewDecoder(bytes.NewBuffer(val))
					//var s string
					var i int
					if err := val.Decode(&i); err != nil {
						t.Log(err.Error())
					}
					t.Log(id, " = ", i)
				}
			}
		}
		if err := n7.DropNode("Deep 7 7"); err != nil {
			t.Log(err.Error())
		} else {
			t.Log("Dropped Deep 7 7")
		}
		if list, err := n7.GetNodes(); err != nil {
			t.Log(err.Error())
		} else {
			if len(list) > 0 {
				for _, nid := range list {
					nn, err := n7.NewNode(nid)
					if err != nil {
						t.Log(err.Error())
						continue
					}
					var body string
					nis, err := nn.GetAll()
					if err != nil {
						t.Log(err.Error())
					} else {
						body += "Items:" + strconv.Itoa(len(nis))
					}
					ees, err := nn.GetNodes()
					if err != nil {
						t.Log(err.Error())
					} else {
						body += " Nodes:" + strconv.Itoa(len(ees))
					}
					t.Log("Node 7 " + nid + ": " + body)
				}
			}
		}
	}

	var nd Device
	if err := db.Get(dev.ID, &nd); err != nil {
		//if err := db.Get(dev.ID, &nd); err != nil {
		t.Error(err.Error())
	} else {
		t.Log("Got Device!", dev.ID, dev.IP.String(), dev.Description, dev.Macs)
	}

}
