package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/TerraScale/ts-cos/internal/cos"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	cfg, err := cos.DefaultConfig()
	if err == nil {
		err = cos.Run(ctx, cfg, os.Args[1:], os.Stdout)
	}
	if err != nil {
		data, _ := json.Marshal(map[string]string{"error": err.Error()})
		fmt.Fprintln(os.Stderr, string(data))
		os.Exit(1)
	}
}
