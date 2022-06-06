#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

GOPATH="${GOPATH:-"$HOME/go"}"
SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")

protoc "${SCRIPT_ROOT}"/informer.proto --gogofaster_out="${GOPATH}"/src --proto_path="${GOPATH}"/src

protoc "${SCRIPT_ROOT}"/kubernetes-api.proto --gogofaster_out="${GOPATH}"/src --proto_path="${GOPATH}"/src