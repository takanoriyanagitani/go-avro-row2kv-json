package row2kj

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"strings"

	ar "github.com/takanoriyanagitani/go-avro-row2kv-json"
	. "github.com/takanoriyanagitani/go-avro-row2kv-json/util"
)

var (
	ErrKeyMissing error = errors.New("key missing")
)

type KeyValueToMap[K any] func(ar.KeyVal[K]) IO[map[string]any]

func ConfigToKeyValueToMap[K any](cfg ar.KeyValConfig) KeyValueToMap[K] {
	buf := map[string]any{}
	return func(kv ar.KeyVal[K]) IO[map[string]any] {
		return func(_ context.Context) (map[string]any, error) {
			clear(buf)

			buf[string(cfg.Keyname)] = kv.Key
			buf[string(cfg.Valname)] = kv.Val

			return buf, nil
		}
	}
}

func ConfigToKeyValueToMapDefault[K any]() KeyValueToMap[K] {
	return ConfigToKeyValueToMap[K](ar.KeyValConfigDefault)
}

type RowToKeyJson[K any] func(ar.OriginalRow) IO[ar.KeyVal[K]]

type RowToKeyJsonToMap func(ar.OriginalRow) IO[map[string]any]

func (c RowToKeyJsonToMap) MapsToKeyValues(
	original iter.Seq2[map[string]any, error],
) IO[iter.Seq2[map[string]any, error]] {
	return func(ctx context.Context) (iter.Seq2[map[string]any, error], error) {
		return func(yield func(map[string]any, error) bool) {
			for row, e := range original {
				if nil != e {
					yield(map[string]any{}, e)
					return
				}

				converted, e := c(row)(ctx)
				if !yield(converted, e) {
					return
				}
			}
		}, nil
	}
}

func (r RowToKeyJson[K]) ToConverter(kv2m KeyValueToMap[K]) RowToKeyJsonToMap {
	return func(o ar.OriginalRow) IO[map[string]any] {
		return Bind(
			r(o),
			kv2m,
		)
	}
}

func (r RowToKeyJson[K]) ConfigToConverter(
	cfg ar.KeyValConfig,
) RowToKeyJsonToMap {
	return r.ToConverter(ConfigToKeyValueToMap[K](cfg))
}

func (r RowToKeyJson[K]) ToConverterDefault() RowToKeyJsonToMap {
	return r.ConfigToConverter(ar.KeyValConfigDefault)
}

type RowToKey[K any] func(ar.OriginalRow) IO[K]
type RowToAny RowToKey[any]

type RowToJson func(ar.OriginalRow) IO[string]

func (r RowToKey[K]) ToRowToKeyJson(r2j RowToJson) RowToKeyJson[K] {
	return func(o ar.OriginalRow) IO[ar.KeyVal[K]] {
		return Bind(
			r(o),
			func(k K) IO[ar.KeyVal[K]] {
				return Bind(
					r2j(o),
					Lift(func(j string) (ar.KeyVal[K], error) {
						return ar.KeyVal[K]{
							Key: k,
							Val: j,
						}, nil
					}),
				)
			},
		)
	}
}

func (r RowToAny) ToRowToKeyJson(r2j RowToJson) RowToKeyJson[any] {
	return RowToKey[any](r).ToRowToKeyJson(r2j)
}

type KeyName ar.Keyname

var KeyNameDefault KeyName = KeyName(ar.KeynameDefault)

func (k KeyName) ToRowToAny() RowToAny {
	return func(r ar.OriginalRow) IO[any] {
		return func(_ context.Context) (any, error) {
			key, found := r[string(k)]
			switch found {
			case true:
				return key, nil
			default:
				return nil, ErrKeyMissing
			}
		}
	}
}

func (k KeyName) ToRowToKeyJson(r2j RowToJson) RowToKeyJson[any] {
	return k.ToRowToAny().ToRowToKeyJson(r2j)
}

func RowToJsonStdNew() RowToJson {
	var buf strings.Builder
	return func(o ar.OriginalRow) IO[string] {
		return func(_ context.Context) (string, error) {
			buf.Reset()
			var m map[string]any = o
			encoded, e := json.Marshal(m)
			_, _ = buf.Write(encoded) // error is always nil or OOM
			return buf.String(), e
		}
	}
}

func (k KeyName) ToRowToKeyJsonStdNew() RowToKeyJson[any] {
	return k.ToRowToKeyJson(RowToJsonStdNew())
}

func (k KeyName) ToConverterJsonStd(cfg ar.KeyValConfig) RowToKeyJsonToMap {
	return k.ToRowToKeyJsonStdNew().ConfigToConverter(cfg)
}

func (k KeyName) ToConverterJsonStdDefault() RowToKeyJsonToMap {
	return k.ToConverterJsonStd(ar.KeyValConfigDefault)
}
