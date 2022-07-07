// Package openapi provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen version v1.9.0 DO NOT EDIT.
package openapi

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/deepmap/oapi-codegen/pkg/runtime"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/gin-gonic/gin"
)

// ServerInterface represents all server handlers.
type ServerInterface interface {
	// delete binlog operator
	// (DELETE /binlog/tasks/{task-name})
	DMAPIDeleteBinlogOperator(c *gin.Context, taskName string, params DMAPIDeleteBinlogOperatorParams)
	// get binlog operator
	// (GET /binlog/tasks/{task-name})
	DMAPIGetBinlogOperator(c *gin.Context, taskName string, params DMAPIGetBinlogOperatorParams)
	// set binlog operator
	// (POST /binlog/tasks/{task-name})
	DMAPISetBinlogOperator(c *gin.Context, taskName string)
	// get job config
	// (GET /config)
	DMAPIGetJobConfig(c *gin.Context)
	// update job config
	// (PUT /config)
	DMAPIUpdateJobConfig(c *gin.Context)
	// get schema
	// (GET /schema/tasks/{task-name})
	DMAPIGetSchema(c *gin.Context, taskName string, params DMAPIGetSchemaParams)
	// set schema
	// (PUT /schema/tasks/{task-name})
	DMAPISetSchema(c *gin.Context, taskName string)
	// get the current status of the job
	// (GET /status)
	DMAPIGetJobStatus(c *gin.Context)
	// get the current status of the task
	// (GET /status/tasks/{task-name})
	DMAPIGetTaskStatus(c *gin.Context, taskName string)
	// operate the stage of a data source
	// (PUT /status/tasks/{task-name})
	DMAPIOperateTask(c *gin.Context, taskName string)
}

// ServerInterfaceWrapper converts contexts to parameters.
type ServerInterfaceWrapper struct {
	Handler            ServerInterface
	HandlerMiddlewares []MiddlewareFunc
}

type MiddlewareFunc func(c *gin.Context)

// DMAPIDeleteBinlogOperator operation middleware
func (siw *ServerInterfaceWrapper) DMAPIDeleteBinlogOperator(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// Parameter object where we will unmarshal all parameters from the context
	var params DMAPIDeleteBinlogOperatorParams

	// ------------- Optional query parameter "binlog_pos" -------------
	if paramValue := c.Query("binlog_pos"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "binlog_pos", c.Request.URL.Query(), &params.BinlogPos)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter binlog_pos: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIDeleteBinlogOperator(c, taskName, params)
}

// DMAPIGetBinlogOperator operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetBinlogOperator(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// Parameter object where we will unmarshal all parameters from the context
	var params DMAPIGetBinlogOperatorParams

	// ------------- Optional query parameter "binlog_pos" -------------
	if paramValue := c.Query("binlog_pos"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "binlog_pos", c.Request.URL.Query(), &params.BinlogPos)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter binlog_pos: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetBinlogOperator(c, taskName, params)
}

// DMAPISetBinlogOperator operation middleware
func (siw *ServerInterfaceWrapper) DMAPISetBinlogOperator(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPISetBinlogOperator(c, taskName)
}

// DMAPIGetJobConfig operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetJobConfig(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetJobConfig(c)
}

// DMAPIUpdateJobConfig operation middleware
func (siw *ServerInterfaceWrapper) DMAPIUpdateJobConfig(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIUpdateJobConfig(c)
}

// DMAPIGetSchema operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetSchema(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	// Parameter object where we will unmarshal all parameters from the context
	var params DMAPIGetSchemaParams

	// ------------- Optional query parameter "database" -------------
	if paramValue := c.Query("database"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "database", c.Request.URL.Query(), &params.Database)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter database: %s", err)})
		return
	}

	// ------------- Optional query parameter "table" -------------
	if paramValue := c.Query("table"); paramValue != "" {
	}

	err = runtime.BindQueryParameter("form", true, false, "table", c.Request.URL.Query(), &params.Table)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter table: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetSchema(c, taskName, params)
}

// DMAPISetSchema operation middleware
func (siw *ServerInterfaceWrapper) DMAPISetSchema(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPISetSchema(c, taskName)
}

// DMAPIGetJobStatus operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetJobStatus(c *gin.Context) {
	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetJobStatus(c)
}

// DMAPIGetTaskStatus operation middleware
func (siw *ServerInterfaceWrapper) DMAPIGetTaskStatus(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIGetTaskStatus(c, taskName)
}

// DMAPIOperateTask operation middleware
func (siw *ServerInterfaceWrapper) DMAPIOperateTask(c *gin.Context) {
	var err error

	// ------------- Path parameter "task-name" -------------
	var taskName string

	err = runtime.BindStyledParameter("simple", false, "task-name", c.Param("task-name"), &taskName)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"msg": fmt.Sprintf("Invalid format for parameter task-name: %s", err)})
		return
	}

	for _, middleware := range siw.HandlerMiddlewares {
		middleware(c)
	}

	siw.Handler.DMAPIOperateTask(c, taskName)
}

// GinServerOptions provides options for the Gin server.
type GinServerOptions struct {
	BaseURL     string
	Middlewares []MiddlewareFunc
}

// RegisterHandlers creates http.Handler with routing matching OpenAPI spec.
func RegisterHandlers(router *gin.Engine, si ServerInterface) *gin.Engine {
	return RegisterHandlersWithOptions(router, si, GinServerOptions{})
}

// RegisterHandlersWithOptions creates http.Handler with additional options
func RegisterHandlersWithOptions(router *gin.Engine, si ServerInterface, options GinServerOptions) *gin.Engine {
	wrapper := ServerInterfaceWrapper{
		Handler:            si,
		HandlerMiddlewares: options.Middlewares,
	}

	router.DELETE(options.BaseURL+"/binlog/tasks/:task-name", wrapper.DMAPIDeleteBinlogOperator)

	router.GET(options.BaseURL+"/binlog/tasks/:task-name", wrapper.DMAPIGetBinlogOperator)

	router.POST(options.BaseURL+"/binlog/tasks/:task-name", wrapper.DMAPISetBinlogOperator)

	router.GET(options.BaseURL+"/config", wrapper.DMAPIGetJobConfig)

	router.PUT(options.BaseURL+"/config", wrapper.DMAPIUpdateJobConfig)

	router.GET(options.BaseURL+"/schema/tasks/:task-name", wrapper.DMAPIGetSchema)

	router.PUT(options.BaseURL+"/schema/tasks/:task-name", wrapper.DMAPISetSchema)

	router.GET(options.BaseURL+"/status", wrapper.DMAPIGetJobStatus)

	router.GET(options.BaseURL+"/status/tasks/:task-name", wrapper.DMAPIGetTaskStatus)

	router.PUT(options.BaseURL+"/status/tasks/:task-name", wrapper.DMAPIOperateTask)

	return router
}

// Base64 encoded, gzipped, json marshaled Swagger object
var swaggerSpec = []string{
	"H4sIAAAAAAAC/+xYS4/bNhD+K8K0RyU22px0a5qiSIGgRTdFD4GxoOSRlrsSSXPIg2HovxccyrJlU463",
	"yCaLJCfLfMzr++Yh7aDSndEKlSModkDVHXaCH3+zVtt/pbt7h0SiwbC2RqqsNE5qBQVog1aE5wzDWcjB",
	"2LDmJLIEXr2t9Dpxl/cy3svBbQ1CAVI5bNBCnw9XO2rmbnaDUeNlclaqBvo+B4sbLy2uofhwJCg/tmc1",
	"3tPlPVYu6PyT3cH3gh7+xo1HckH51CVt2DHluyDcIvku2GCEp2OhM8Zok1R8g+61VK1uogXazqov+dit",
	"0fzvRFl+Yh09SAPBANOKKlgpFWs8NzMH2rQsUjrs0rKHBWGt2D7asRum1axba+FEKQiTimuru1vS3lYD",
	"jWrhWwdFLVrCUWmpdYtCjRecsA266y7Qpk27LMo2ZdOJ86P1+xtRYiog/5i1cPiHLn/VqpbNbEAq3v64",
	"6uHcuapwUKpaQ6F82wZmoBJGQgE/v1y+XDJl3R3rWkRSLZygB1rsws8LJTrsY/BadBiZPyT72zUU8Obd",
	"L3+9fcObU+qyZCs6dGgJig+n2RvEZ0E88xEKtgNy4KW4/WLYPjjqrMd8qE3JoOyirI1Huz0IO8qWS7dX",
	"QRUZrSgG/6flq/OqQ76qkCiA+Gq5HDByqBg8YUwrK47O4p7C+d2Rvh8t1lDAD4tDqV0MdXZxVmQZuanq",
	"WsgW14w9+a4TdgvFAEwWXcz0IfpONCHuHElY9TkMaZDA7/fTunMO3rOF6HEQDPJ4IxHh5wZug+4aZI2m",
	"OWhvPg7tZ8jLVTyM5F7r9faTxXW2XybiO+jPymBA/23RiK6hUZ/D4tBuLhaLsW/B08bxXpcxhok6HE3N",
	"lHZZrb1KZc69LrNqb+fe2yCTc8bPOXjSm+FpuDszAfTTzh4Sa4asz5tynt27BEHgW1STnjguMjDOkV9q",
	"xphq2Y99E00n7e1oNHyEZJ4iL4kdx8zvDfOQ9rTnxnmf9Bfa5Bek1FO3x+lb1/Ul5mvuh0mW0L4uOeE8",
	"XdMHb+LJby/L3B1mlbcWlctiuDJd82oo78liz6f+R7F/L+hhjPPF7GxaXYq23WZeyY3HLJTdLH4yeJqk",
	"/Y55xJxL7KMq7tFHtmeH6qcvxYlPil/LpBexRaYBOdFg4IQ4BinxrtGPS7vEzD9iGF8AUj14ijP0q/6/",
	"AAAA//9XWeYawhYAAA==",
}

// GetSwagger returns the content of the embedded swagger specification file
// or error if failed to decode
func decodeSpec() ([]byte, error) {
	zipped, err := base64.StdEncoding.DecodeString(strings.Join(swaggerSpec, ""))
	if err != nil {
		return nil, fmt.Errorf("error base64 decoding spec: %s", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %s", err)
	}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(zr)
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %s", err)
	}

	return buf.Bytes(), nil
}

var rawSpec = decodeSpecCached()

// a naive cached of a decoded swagger spec
func decodeSpecCached() func() ([]byte, error) {
	data, err := decodeSpec()
	return func() ([]byte, error) {
		return data, err
	}
}

// Constructs a synthetic filesystem for resolving external references when loading openapi specifications.
func PathToRawSpec(pathToFile string) map[string]func() ([]byte, error) {
	res := make(map[string]func() ([]byte, error))
	if len(pathToFile) > 0 {
		res[pathToFile] = rawSpec
	}

	return res
}

// GetSwagger returns the Swagger specification corresponding to the generated code
// in this file. The external references of Swagger specification are resolved.
// The logic of resolving external references is tightly connected to "import-mapping" feature.
// Externally referenced files must be embedded in the corresponding golang packages.
// Urls can be supported but this task was out of the scope.
func GetSwagger() (swagger *openapi3.T, err error) {
	resolvePath := PathToRawSpec("")

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	loader.ReadFromURIFunc = func(loader *openapi3.Loader, url *url.URL) ([]byte, error) {
		pathToFile := url.String()
		pathToFile = path.Clean(pathToFile)
		getSpec, ok := resolvePath[pathToFile]
		if !ok {
			err1 := fmt.Errorf("path not found: %s", pathToFile)
			return nil, err1
		}
		return getSpec()
	}
	var specData []byte
	specData, err = rawSpec()
	if err != nil {
		return
	}
	swagger, err = loader.LoadFromData(specData)
	if err != nil {
		return
	}
	return
}
