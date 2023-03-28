package apiregistry

import (
	"fmt"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/gin-gonic/gin"
)

const defaultTimeout = 30 * time.Second

type registerHttp struct {
	group       string
	method      string
	path        string
	handler     gin.HandlerFunc
	middlewares []gin.HandlerFunc
}

type registerHttpRsp struct {
	group  string
	method string
	path   string
}

type HTTPRegistryByDefault interface {
	RegisterHttpByDefaultGroup(method string, relativePath string,
		handler gin.HandlerFunc, middleware ...gin.HandlerFunc) error
}

type HTTPRegistry interface {
	RegisterHttp(groupPath string, method string, relativePath string,
		handler gin.HandlerFunc, middleware ...gin.HandlerFunc) error
}

type HTTPRegistryBuilder interface {
	SetDefaultGroup(path string) HTTPRegistryByDefault
	HTTPRegistry
}

type registry struct {
	ctx    actor.Context
	apiPID *actor.PID

	defaultPath string
}

func NewRegistry(ctx actor.Context, apiPID *actor.PID) HTTPRegistryBuilder {
	return &registry{
		ctx:         ctx,
		apiPID:      apiPID,
		defaultPath: "/",
	}
}

func (r *registry) SetDefaultGroup(path string) HTTPRegistryByDefault {
	r.defaultPath = path
	return r
}

func (r *registry) RegisterHttp(groupPath string, method string,
	relativePath string, handler gin.HandlerFunc, middleware ...gin.HandlerFunc) error {
	result, err := r.ctx.RequestFuture(r.apiPID, &registerHttp{
		group:       groupPath,
		method:      method,
		path:        relativePath,
		handler:     handler,
		middlewares: middleware,
	}, defaultTimeout).Result()

	if err != nil {
		return fmt.Errorf("register http group %s, %s %s timeout",
			groupPath, method, relativePath)
	}

	if err, ok := result.(error); ok && err != nil {
		return fmt.Errorf("register http group %s, %s %s fail due to %v",
			groupPath, method, relativePath, err)
	}
	return nil
}

func (r *registry) RegisterHttpByDefaultGroup(method string, relativePath string,
	handler gin.HandlerFunc, middleware ...gin.HandlerFunc) error {
	return r.RegisterHttp(r.defaultPath, method, relativePath, handler, middleware...)
}
