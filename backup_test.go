package database

import (
	"archive/zip"
	"context"
	"os"
	"testing"
	"time"
)

func TestBackup(t *testing.T) {
	db, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := db.Set("Test", "Value"); err != nil {
		t.Log("error setting Test:", err.Error())
	}
	if err := db.Set("Test", "Update"); err != nil {
		t.Error(err.Error())
	}
	ndb, err := db.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := ndb.SetValue("TEST2", []byte("TEST2")); err != nil {
		t.Error(err.Error())
		return
	}
	//file, err := os.OpenFile("Database.bak", os.O_TRUNC|os.O_CREATE, 0777)
	file, err := os.Create("Database.bak")
	if err != nil {
		t.Error(err.Error())
		db.Close()
		return
	}
	defer file.Close()
	wr := zip.NewWriter(file)
	if err := db.Backup(wr); err != nil {
		t.Error(err.Error())
		db.Close()
		return
	}
	wr.Close()

	if err := db.Close(); err != nil {
		t.Log(err.Error())
	}
}

func TestBackupRestore(t *testing.T) {
	db, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer db.Close()
	if err := db.Set("Test", "Value"); err != nil {
		t.Log("error setting Test:", err.Error())
	}
	if err := db.Set("Test", "Update"); err != nil {
		t.Error(err.Error())
	}
	ndb, err := db.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := ndb.SetValue("TEST2", []byte("TEST2")); err != nil {
		t.Error(err.Error())
		return
	}
	//file, err := os.OpenFile("Database.bak", os.O_TRUNC|os.O_CREATE, 0777)
	file, err := os.Create("Database.bak")
	if err != nil {
		t.Error(err.Error())
		return
	}

	wr := zip.NewWriter(file)
	if err := db.Backup(wr); err != nil {
		t.Error(err.Error())
		return
	}
	wr.Close()
	file.Close()

	newdb, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer newdb.Close()

	rdr, err := zip.OpenReader("Database.bak")
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer rdr.Close()
	if err := newdb.Restore(&rdr.Reader); err != nil {
		t.Error(err.Error())
		return
	}

	var out string
	if err := newdb.Get("Test", &out); err != nil {
		t.Error(err.Error())
		return
	}
	if out != "Update" {
		t.Error("Test value does not match:", out)
		return
	} else {
		t.Log("Test value restored: ", out)
	}
	nndb, err := newdb.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if out2, err := nndb.GetValue("TEST2"); err != nil {
		t.Error(err.Error())
		return
	} else {
		if string(out2) != "TEST2" {
			t.Error("Test2 value does not match:", string(out2))
			return
		} else {
			t.Log("Test2 value restored: ", string(out2))
		}
	}
}

func TestBackupRestoreNode(t *testing.T) {
	db, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer db.Close()
	if err := db.Set("Test", "Value"); err != nil {
		t.Log("error setting Test:", err.Error())
	}
	if err := db.Set("Test", "Update"); err != nil {
		t.Error(err.Error())
	}
	ndb, err := db.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := ndb.SetValue("TEST2", []byte("TEST2")); err != nil {
		t.Error(err.Error())
		return
	}
	//file, err := os.OpenFile("Database.bak", os.O_TRUNC|os.O_CREATE, 0777)
	file, err := os.Create("Database2.bak")
	if err != nil {
		t.Error(err.Error())
		return
	}

	wr := zip.NewWriter(file)
	if err := ndb.Backup(wr); err != nil {
		t.Error(err.Error())
		return
	}
	wr.Close()
	file.Close()

	newdb, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer newdb.Close()

	rdr, err := zip.OpenReader("Database2.bak")
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer rdr.Close()
	/*if err := newdb.Restore(&rdr.Reader); err != nil {
		t.Error(err.Error())
		return
	}*/

	nndb, err := newdb.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}

	if err := nndb.Restore(&rdr.Reader); err != nil {
		t.Error(err.Error())
		return
	}

	var out string
	if err := newdb.Get("Test", &out); err != nil {
		t.Log("Value outside of node not restored")
	} else {
		t.Error("Value outside of node was restored", "Test", out)
	}

	if out2, err := nndb.GetValue("TEST2"); err != nil {
		t.Error(err.Error())
		return
	} else {
		if string(out2) != "TEST2" {
			t.Error("Test2 value does not match:", string(out2))
			return
		} else {
			t.Log("Test2 value restored: ", string(out2))
		}
	}
}

func TestBackupRestoreNodeDB(t *testing.T) {
	db, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer db.Close()
	if err := db.Set("Test", "Value"); err != nil {
		t.Log("error setting Test:", err.Error())
	}
	if err := db.Set("Test", "Update"); err != nil {
		t.Error(err.Error())
	}
	ndb, err := db.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}
	if err := ndb.SetValue("TEST2", []byte("TEST2")); err != nil {
		t.Error(err.Error())
		return
	}
	file, err := os.Create("Database3.bak")
	if err != nil {
		t.Error(err.Error())
		return
	}

	wr := zip.NewWriter(file)
	if err := ndb.Backup(wr); err != nil {
		t.Error(err.Error())
		return
	}
	wr.Close()
	file.Close()

	newdb, err := NewInMemoryBadger(context.Background(), 2*time.Second)
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer newdb.Close()

	rdr, err := zip.OpenReader("Database3.bak")
	if err != nil {
		t.Error(err.Error())
		return
	}
	defer rdr.Close()
	if err := newdb.Restore(&rdr.Reader); err != nil {
		t.Error(err.Error())
		return
	}

	nndb, err := newdb.NewNode("Node")
	if err != nil {
		t.Error(err.Error())
		return
	}

	/*if err := nndb.Restore(&rdr.Reader); err != nil {
		t.Error(err.Error())
		return
	}*/

	var out string
	if err := newdb.Get("Test", &out); err != nil {
		t.Log("Value outside of node not restored")
	} else {
		t.Error("Value outside of node was restored", "Test", out)
	}

	if out2, err := nndb.GetValue("TEST2"); err != nil {
		t.Error(err.Error())
		return
	} else {
		if string(out2) != "TEST2" {
			t.Error("Test2 value does not match:", string(out2))
			return
		} else {
			t.Log("Test2 value restored: ", string(out2))
		}
	}
}
