package database

import (
	"bytes"
	"github.com/fxamacker/cbor"
)

type cborEncoder struct {
	data *bytes.Buffer
	enc  DBEncoder
}

type cborDecoder struct {
	data *bytes.Buffer
	dec  DBDecoder
}

func newCborEncoder() Encoder {

	var cd cborEncoder
	cd.data = &bytes.Buffer{}
	cd.enc = cbor.NewEncoder(cd.data, cbor.EncOptions{})

	return &cd
}

func (cb *cborEncoder) Encode(i interface{}) error {
	return cb.enc.Encode(i)
}

func (cb *cborEncoder) Data() []byte {
	return cb.data.Bytes()
}

func newCborDecoder(data []byte) Decoder {
	var cd cborDecoder
	cd.data = bytes.NewBuffer(data)
	cd.dec = cbor.NewDecoder(cd.data)

	return &cd
}

func (cd *cborDecoder) Decode(i interface{}) error {
	return cd.dec.Decode(i)
}

func (cd *cborDecoder) Data() []byte {
	return cd.data.Bytes()
}

func (cd *cborDecoder) Len() uint64 {
	return uint64(cd.data.Len())
}
