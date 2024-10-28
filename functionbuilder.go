package database

type BadgerFunction func([]byte) error

func buildGetBadger(i interface{}) BadgerFunction {
	f := func(val []byte) error {
		if nb, ok := i.([]byte); ok {
			if cap(nb) < len(val) {
				nb = make([]byte, len(val))
			}
			copy(nb, val)
			return nil
		}
		//dcr := gob.NewDecoder(bytes.NewReader(val))
		dcr := newGobDecoder(val)
		if err := dcr.Decode(i); err != nil {
			return err
		}
		return nil
	}
	return f
}

func buildGetBadgerV3(i interface{}) BadgerFunction {
	f := func(val []byte) error {
		if nb, ok := i.([]byte); ok {
			if cap(nb) < len(val) {
				nb = make([]byte, len(val))
			}
			copy(nb, val)
			return nil
		}
		dcr := newCborDecoder(val)
		if err := dcr.Decode(i); err != nil {
			return err
		}
		return nil
	}
	return f
}
