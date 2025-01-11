#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

_install(){
	echo avro2jsons missing.
	echo avro2jsons is available here: github.com/takanoriyanagitani/go-avro2jsons
	echo install command sample: go install -v url/to/command/dir@latest
	exit 1
}

#genavro

which avro2jsons | fgrep -q avro2jsons || _install

export ENV_SCHEMA_FILENAME=./sample.d/output.avsc
export ENV_KEYNAME=name

cat sample.d/sample.avro |
	./avro2keyjson |
	avro2jsons |
	jq -c '{name, val: (.val | fromjson)}'
