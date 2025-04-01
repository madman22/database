package database

import (
	"bytes"
	"encoding/gob"
	"errors"
)

type DBEncoder interface {
	Encode(interface{}) error
}

type Encoder interface {
	Data() []byte
	Encode(interface{}) error
}

type gobEncoder struct {
	data *bytes.Buffer
	enc  DBEncoder
}

type DBDecoder interface {
	Decode(interface{}) error
}

type Decoder interface {
	Data() []byte
	Decode(interface{}) error
	Len() uint64
}

type gobDecoder struct {
	data *bytes.Buffer
	//dec  *gob.Decoder
	dec DBDecoder
}

func newGobDecoder(data []byte) Decoder {
	var gd gobDecoder
	gd.data = bytes.NewBuffer(data)
	gd.dec = gob.NewDecoder(gd.data)
	//gd.dec = NewGobDecoder(gd.data)
	return &gd
}

func (gd *gobDecoder) Len() uint64 {
	return uint64(gd.data.Len())
}

func (gd *gobDecoder) Data() []byte {
	if gd.data == nil {
		return []byte{}
	}
	return gd.data.Bytes()
}

func (gd *gobDecoder) Decode(i interface{}) error {
	if gd.dec == nil {
		return errors.New("Decoder not built!")
	}
	if gd.data == nil {
		return errors.New("Empty data")
	}
	return gd.dec.Decode(i)
}

func newGobEncoder() Encoder {
	var ge gobEncoder
	ge.data = &bytes.Buffer{}
	ge.enc = gob.NewEncoder(ge.data)
	return &ge
}

func (ge *gobEncoder) Data() []byte {
	if ge.data == nil {
		return []byte{}
	}
	return ge.data.Bytes()
}

func (ge *gobEncoder) Encode(i interface{}) error {
	if ge.enc == nil {
		return errors.New("Encoder not built!")
	}
	if ge.data == nil {
		return errors.New("Empty Data")
	}
	return ge.enc.Encode(i)
}

/*func NewGobDecoder(r io.Reader) Decoder {
	return gob.NewDecoder(r)
}*/
