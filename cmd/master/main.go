package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"reelfs/gen/masterpb"
	"reelfs/internal/master"

	"google.golang.org/grpc"
)

func main() {
	grpcPort := flag.String("grpc-port", "50040", "")
	replicationFactor := flag.Int("replicas", 3, "")
	flag.Parse()
	grpcAddr := ":" + *grpcPort
	lookup := master.NewLookupTable(*replicationFactor)
	masterServer := master.NewMasterServer(lookup)
	masterInternalServer := master.NewMasterInternalTestServer(lookup)
	grpcServer := grpc.NewServer()
	masterpb.RegisterMasterServiceServer(grpcServer, masterServer)
	masterpb.RegisterMasterInternalServiceServer(grpcServer, masterInternalServer)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				lookup.RemoveDeadKeepers()
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case <-time.After(5 * time.Second):
				log.Print("**...REPLICATING FILES...**")
				lookup.ReplicateFiles()
				log.Print("**...REPLICATION DONE...**")
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		<-sigCh
		log.Println("shutting down...")
		cancel()
		grpcServer.GracefulStop()
	}()
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("could not listen to grpc: %v", err)
	}
	log.Printf("server listening to grpc on port %s", *grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("gRPC server: %v", err)
	}
	log.Println("server stopped")
}
