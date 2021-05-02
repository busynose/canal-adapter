#!/bin/bash
shell_dir=$(dirname $0)
cd ${shell_dir}

# check params
if [[ ! $1 ]]; then
    echo "api version is null"; exit 1;
else
    echo "api version：$1"
fi

if [[ ! $2 ]]; then
    echo "image tag is null"; exit 1;
else
    echo "image tag：$2"
fi

# build binary
go mod tidy
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-w -s' -o canalAdapter ../../main.go
cp ../../config.yml ./

# build image
DOCKER_BUILDKIT=0 docker build --build-arg tmp_api_version=$1 -t $2 -f Dockerfile .

## docker push
# docker push $2