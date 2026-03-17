package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"reelfs/gen/masterpb"
	"reelfs/internal/master"

	"google.golang.org/grpc"
)

func main() {
	grpcAddr := flag.String("grpc-addr", envOrDefault("GRPC_PORT", ":50053"), "gRPC Listen Address")
	defaultReplicationFactor, _ := strconv.Atoi(envOrDefault("REPLICATION_FACTOR", "3"))
	replicationFactor := flag.Int("replication-factor", defaultReplicationFactor, "Least number of replicas for each files")
	fmt.Println("Master Tracker is starting...")

	lookup := master.NewLookupTable()
	masterInternalServer := master.NewMasterInternalServer(lookup)
	masterServer := master.NewMasterServer(lookup)
	grpcServer := grpc.NewServer()
	masterpb.RegisterMasterInternalServiceServer(grpcServer, masterInternalServer)
	masterpb.RegisterMasterServiceServer(grpcServer, masterServer)
	go func() {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Printf("grpc server: %v", err)
		}
		log.Printf("grpc server listening on %s", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("grpc server: %v", err)
		}
	}()

	go func() { // replication goroutine
		for {
			lookup.SelectAndReplicateFiles(*replicationFactor)
			time.Sleep(10 * time.Second)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		grpcServer.GracefulStop()
		// Terminate Program
		os.Exit(0)
	}()
}

func envOrDefault(envVar, def string) string {
	if v := os.Getenv(envVar); v != "" {
		return v
	}
	return def
}
