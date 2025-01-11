package row2kv

type OriginalRow map[string]any

type KeyVal[K any] struct {
	Key K
	Val string
}

type Keyname string
type Valname string

const KeynameDefault Keyname = "key"
const ValnameDefault Valname = "val"

type KeyValConfig struct {
	Keyname
	Valname
}

var KeyValConfigDefault KeyValConfig = KeyValConfig{
	Keyname: KeynameDefault,
	Valname: ValnameDefault,
}

const BlobSizeMaxDefault int = 1048576

type DecodeConfig struct {
	BlobSizeMax int
}

var DecodeConfigDefault DecodeConfig = DecodeConfig{
	BlobSizeMax: BlobSizeMaxDefault,
}

type Codec string

const (
	CodecNull    Codec = "null"
	CodecDeflate Codec = "deflate"
	CodecSnappy  Codec = "snappy"
	CodecZstd    Codec = "zstandard"
	CodecBzip2   Codec = "bzip2"
	CodecXz      Codec = "xz"
)

const BlockLengthDefault int = 100

type EncodeConfig struct {
	BlockLength int
	Codec
}

var EncodeConfigDefault EncodeConfig = EncodeConfig{
	BlockLength: BlockLengthDefault,
	Codec:       CodecNull,
}
