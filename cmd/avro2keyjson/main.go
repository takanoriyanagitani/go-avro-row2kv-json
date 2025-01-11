package main

import (
	"context"
	"fmt"
	"io"
	"iter"
	"log"
	"os"
	"strings"

	ar "github.com/takanoriyanagitani/go-avro-row2kv-json"
	. "github.com/takanoriyanagitani/go-avro-row2kv-json/util"

	rk "github.com/takanoriyanagitani/go-avro-row2kv-json/row2kj"

	dh "github.com/takanoriyanagitani/go-avro-row2kv-json/avro/dec/hamba"
	eh "github.com/takanoriyanagitani/go-avro-row2kv-json/avro/enc/hamba"
)

var EnvValByKey func(string) IO[string] = Lift(
	func(key string) (string, error) {
		val, found := os.LookupEnv(key)
		switch found {
		case true:
			return val, nil
		default:
			return "", fmt.Errorf("env var %s missing", key)
		}
	},
)

var stdin2maps IO[iter.Seq2[map[string]any, error]] = dh.
	StdinToMapsDefault

var keyName IO[string] = EnvValByKey("ENV_KEYNAME").
	Or(Of(string(ar.KeynameDefault)))

var valName IO[string] = EnvValByKey("ENV_VALNAME").
	Or(Of(string(ar.ValnameDefault)))

var kvConfig IO[ar.KeyValConfig] = Bind(
	All(
		keyName,
		valName,
	),
	Lift(func(s []string) (ar.KeyValConfig, error) {
		return ar.KeyValConfig{
			Keyname: ar.Keyname(s[0]),
			Valname: ar.Valname(s[1]),
		}, nil
	}),
).Or(Of(ar.KeyValConfigDefault))

var row2keyJson2map IO[rk.RowToKeyJsonToMap] = Bind(
	keyName,
	func(key string) IO[rk.RowToKeyJsonToMap] {
		return Bind(
			kvConfig,
			Lift(func(
				cfg ar.KeyValConfig,
			) (rk.RowToKeyJsonToMap, error) {
				return rk.KeyName(key).ToConverterJsonStd(cfg), nil
			}),
		)
	},
)

var mapd IO[iter.Seq2[map[string]any, error]] = Bind(
	row2keyJson2map,
	func(r2kj2m rk.RowToKeyJsonToMap) IO[iter.Seq2[map[string]any, error]] {
		return Bind(
			stdin2maps,
			r2kj2m.MapsToKeyValues,
		)
	},
)

var schemaFilename IO[string] = EnvValByKey("ENV_SCHEMA_FILENAME")

func FilenameToStringLimited(limit int64) func(string) IO[string] {
	return Lift(func(filename string) (string, error) {
		f, e := os.Open(filename)
		if nil != e {
			return "", e
		}

		limited := &io.LimitedReader{
			R: f,
			N: limit,
		}

		var buf strings.Builder
		_, e = io.Copy(&buf, limited)
		return buf.String(), e
	})
}

const SchemaFileSizeMaxDefault int64 = 1048576

var schemaContent IO[string] = Bind(
	schemaFilename,
	FilenameToStringLimited(SchemaFileSizeMaxDefault),
)

var stdin2avro2maps2mapd2avro2stdout IO[Void] = Bind(
	schemaContent,
	func(schema string) IO[Void] {
		return Bind(
			mapd,
			eh.SchemaToMapsToStdoutDefault(schema),
		)
	},
)

var sub IO[Void] = func(ctx context.Context) (Void, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	return stdin2avro2maps2mapd2avro2stdout(ctx)
}

func main() {
	_, e := sub(context.Background())
	if nil != e {
		log.Printf("%v\n", e)
	}
}
