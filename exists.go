package database

func (db *BadgerDB) Exists(id string) bool {
	if db.db == nil {
		return false
	}
	tx := db.db.NewTransaction(false)
	defer tx.Discard()
	_, err := tx.Get([]byte(EntityPrefix + id))
	if err != nil {
		return false
	}
	return true
}

func (db *BadgerNode) Exists(id string) bool {
	if db.db == nil {
		return false
	}
	tx := db.db.NewTransaction(false)
	defer tx.Discard()
	_, err := tx.Get([]byte(db.prefix + EntityPrefix + id))
	if err != nil {
		return false
	}
	return true
}

func (db *BadgerExpiry) Exists(id string) bool {
	if db.db == nil {
		return false
	}
	tx := db.db.NewTransaction(false)
	defer tx.Discard()
	_, err := tx.Get([]byte(db.prefix + EntityPrefix + id))
	if err != nil {
		return false
	}
	return true
}
