# database
A Madman's abstraction to badger database.


Abstration layer on top of badger.  Provides nodes and other convenience methods.

Example Client Usage:
```
type inventoryDB struct {
	db        database.Database
	macs      database.Database
	customers database.Database
}

type macData struct {
	CustomerID string
	DeviceID   string
}

type invCustomerData struct {
	Devices []string
}

func newInventoryDatabase(db database.Database) (*inventoryDB, error) {
	if db == nil {
		return nil, ErrorDatabaseNil
	}
	node, err := db.NewNode("Inventory")
	if err != nil {
		return nil, err
	}
	macs, err := node.NewNode("Macs")
	if err != nil {
		return nil, err
	}
	custs, err := node.NewNode("Customers")
	if err != nil {
		return nil, err
	}
	var cdb inventoryDB
	cdb.db = node
	cdb.macs = macs
	cdb.customers = custs
	return &cdb, nil
}

func (cdb *inventoryDB) ByMac(mac net.HardwareAddr) (device.Device, error) {
	if cdb.macs == nil {
		return device.Device{}, ErrorDatabaseNil
	}
	var data macData
	if err := cdb.macs.Get(mac.String(), &data); err != nil {
		return device.Device{}, errors.New("Mac not found " + mac.String() + ": " + err.Error())
	}
	if data.DeviceID == "" {
		return device.Device{}, errors.New("Missing Device ID")
	}
	dev, err := cdb.ByID(data.DeviceID)
	if err != nil {
		return device.Device{}, errors.New("Bad Mac Lookup:" + mac.String() + " " + data.DeviceID + " " + data.CustomerID)
	}
	return dev, nil
}
```
