#!/bin/sh
GOPATH=$(cd $(dirname $0) && pwd)/weed-fs
cd $GOPATH/src || exit 1
go fmt cmd/weed/*.go
for nm in pkg/*; do go fmt $nm/*.go; done
cd cmd/weed || exit 2
go build -o ../../../bin/weed "$@"
