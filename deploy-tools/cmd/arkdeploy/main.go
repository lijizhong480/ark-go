package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	deploy "github.com/lijizhong480/ark-go/deploy-tools"
)

func main() {
	if len(os.Args) < 2 {
		fail("usage: arkdeploy build|package|start|stop -service-dir DIR")
	}
	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	serviceDir := flags.String("service-dir", ".", "service module directory")
	archive := flags.String("archive", "", "package output tar.gz")
	_ = flags.Parse(os.Args[2:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var err error
	switch command {
	case "build":
		var path string
		path, err = deploy.Build(ctx, *serviceDir)
		if err == nil {
			fmt.Println(path)
		}
	case "package":
		err = deploy.Package(ctx, *serviceDir, *archive)
	case "start":
		var pid int
		pid, err = deploy.Start(ctx, deploy.StartOptions{ServiceDir: *serviceDir})
		if err == nil {
			fmt.Println(pid)
		}
	case "stop":
		err = deploy.Stop(ctx, *serviceDir)
	default:
		fail("unknown command: " + command)
	}
	if err != nil {
		fail(err.Error())
	}
}

func fail(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
