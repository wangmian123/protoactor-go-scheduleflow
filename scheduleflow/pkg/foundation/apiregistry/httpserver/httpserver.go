package httpserver

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/gin-gonic/gin"
	cmap "github.com/orcaman/concurrent-map"
	"github.com/sirupsen/logrus"
	ginglog "github.com/szuecs/gin-glog"
	"go.mongodb.org/mongo-driver/mongo"
)

const (
	logPrefix = "[HttpServer]"
)

func validMethod() map[string]struct{} {
	return map[string]struct{}{
		http.MethodGet:     {},
		http.MethodPost:    {},
		http.MethodPut:     {},
		http.MethodPatch:   {},
		http.MethodHead:    {},
		http.MethodOptions: {},
		http.MethodDelete:  {},
		http.MethodConnect: {},
		http.MethodTrace:   {},
	}
}

type Httpserver struct {
	registry    []string
	validMethod map[string]struct{}

	mongo *mongo.Collection

	server  *http.Server
	handler *gin.Engine
	groups  cmap.ConcurrentMap[*gin.RouterGroup]
}

func NewHttpServer(mongo *mongo.Collection) *Httpserver {
	server := &Httpserver{
		mongo:       mongo,
		groups:      cmap.New[*gin.RouterGroup](),
		validMethod: validMethod(),
	}
	server.handler = gin.New()
	server.handler.Use(ginglog.Logger(3 * time.Second))
	server.handler.Use(gin.Recovery())
	server.server = &http.Server{
		Addr:    ":8080",
		Handler: server.handler,
	}
	return server
}

func (h *Httpserver) ListenAndServe() {
	logrus.Infof("%s start run server at %v", logPrefix, h.server.Addr)

	h.handler.Handle(http.MethodGet, "/", h.root)
	h.registry = append(h.registry, "GET /")

	err := h.server.ListenAndServe()
	if err != nil {
		logrus.Fatalf("%s can not start server due to: %v", logPrefix, err)
	}
	logrus.Infof("%s server closed", logPrefix)
}

func (h *Httpserver) root(ctx *gin.Context) {
	type info struct {
		Title       string   `json:"Title"`
		Information []string `json:"Information"`
	}

	newInfo := info{Title: "path information", Information: make([]string, 0, len(h.registry))}
	newInfo.Information = append(newInfo.Information, h.registry...)
	ctx.JSON(http.StatusOK, newInfo)
}

func (h *Httpserver) shutdown() {
	err := h.server.Shutdown(context.Background())
	if err != nil {
		logrus.Errorf("%s can not shutdown server", logPrefix)
	}
}

func (h *Httpserver) RegisterHttpHandler(parentPath string, method string,
	relativePath string, handler gin.HandlerFunc, middleware ...gin.HandlerFunc) error {
	err := h.checkMethodValid(method)
	if err != nil {
		return err
	}
	group, ok := h.groups.Get(parentPath)
	if !ok {
		group = h.handler.Group(parentPath)
	}
	group.Handle(method, relativePath, handler).Use(middleware...)
	absolutePath := joinPaths(parentPath, relativePath)
	h.registry = append(h.registry, method+" "+absolutePath)
	return nil
}

func (h *Httpserver) checkMethodValid(method string) error {
	_, ok := h.validMethod[method]
	if !ok {
		return fmt.Errorf("can not add handler due to valid method")
	}
	return nil
}

func joinPaths(absolutePath, relativePath string) string {
	if relativePath == "" {
		return absolutePath
	}

	finalPath := path.Join(absolutePath, relativePath)
	if lastChar(relativePath) == '/' && lastChar(finalPath) != '/' {
		return finalPath + "/"
	}
	return finalPath
}

func lastChar(str string) uint8 {
	if str == "" {
		panic("The length of the string can't be 0")
	}
	return str[len(str)-1]
}
