package main

import (
	"flag"
	"fmt"
	"github.com/MolCrafts/molq/protocol"
	"github.com/MolCrafts/molq/registry"
	"github.com/MolCrafts/molq/rpc"
	"github.com/MolCrafts/molq/serve"
	"github.com/MolCrafts/molq/transport"
	"net"
	"net/http"
	"os"
)

func runServe(args []string) error {
	if len(args) == 1 && args[0] == "--file-io" {
		return transport.FileIO(os.Stdin, os.Stdout)
	}
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	stdio := flags.Bool("stdio", false, "serve RPC on stdin and stdout")
	configPath := flags.String("config", "", "configuration file")
	registryPath := flags.String("registry", "", "registry database")
	host := flags.String("host", "", "listen address for this start")
	port := flags.Int("port", 0, "listen port for this start")
	if err := flags.Parse(args); err != nil {
		return protocol.Fail("INVALID_INPUT", err.Error(), nil)
	}
	file, resolved, err := resolveConfig(*configPath)
	if err != nil {
		return err
	}
	path := *registryPath
	if path == "" {
		path = file.Client.Registry
	}
	if path == "" {
		path = registry.DefaultPath()
	}
	service, err := serve.Open(path)
	if err != nil {
		return err
	}
	defer service.Close()
	if err := service.UseConfig(resolved); err != nil {
		return err
	}
	if *stdio {
		return rpc.Stdio(service, os.Stdin, os.Stdout)
	}
	listenHost := *host
	if listenHost == "" {
		listenHost = file.Serve.Host
	}
	if listenHost == "" {
		listenHost = "127.0.0.1"
	}
	listenPort := *port
	if listenPort == 0 {
		listenPort = file.Serve.Port
	}
	if listenPort == 0 {
		listenPort = 17891
	}
	handler, err := rpc.Handler(service, file.Serve.Token)
	if err != nil {
		return err
	}
	return http.ListenAndServe(net.JoinHostPort(listenHost, fmt.Sprint(listenPort)), handler)
}
