package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mayahiro/mysqlmock/pkg/mysqlmock"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return usage()
	}

	switch args[0] {
	case "serve":
		return serve(args[1:])
	case "check":
		return check(args[1:])
	case "dump-unsupported-template":
		fmt.Println(mysqlmock.UnsupportedTemplate())
		return nil
	default:
		return usage()
	}
}

func usage() error {
	return fmt.Errorf("usage: mysqlmock <serve|check|dump-unsupported-template> [options]")
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "YAML config path")
	listen := fs.String("listen", "", "listen address override")
	printDSN := fs.Bool("print-dsn", false, "print DSN after startup")
	verbose := fs.Bool("verbose", false, "enable query logs")
	_ = fs.Bool("fail-on-unsupported", false, "reserved for future use")
	_ = fs.String("log-format", "text", "reserved for future use")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return fmt.Errorf("--config is required")
	}

	opts := []mysqlmock.Option{mysqlmock.ConfigFile(*configPath)}
	if *listen != "" {
		opts = append(opts, mysqlmock.Listen(*listen))
	}
	if *verbose {
		opts = append(opts, mysqlmock.LogWriter(os.Stderr))
	}

	server, err := mysqlmock.New(opts...)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := server.Start(ctx); err != nil {
		return err
	}
	defer server.Close()

	if *printDSN {
		fmt.Println(server.DSN())
	}

	<-ctx.Done()
	return nil
}

func check(args []string) error {
	fs := flag.NewFlagSet("check", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "YAML config path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *configPath == "" {
		return fmt.Errorf("--config is required")
	}
	return mysqlmock.CheckConfigFile(context.Background(), *configPath)
}
