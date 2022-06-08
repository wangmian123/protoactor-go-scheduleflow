package utils

import "github.com/asynkron/protoactor-go/actor"

func FormActorKey(pid *actor.PID) string {
	return pid.Address + "/" + pid.Id
}
