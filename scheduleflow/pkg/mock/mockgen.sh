#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

GOPATH="${GOPATH:-"$HOME/go"}"
SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")

mockgen -destination scheduler_mock.go -package mock codehub-y.huawei.com/multi-clusters-controller/pkg/scheduler/framework  Framework,ClusterInformationProvider