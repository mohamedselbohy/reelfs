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

	"reelfs/gen/keeperpb"
	"reelfs/gen/masterpb"
	"reelfs/internal/keeper"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	tcpPort := flag.String("tcp-port", envOrDefault("TCP_PORT", ":50052"), "TCP Listen Address")
	grpcPort := flag.String("grpc-port", envOrDefault("GRPC_PORT", ":50053"), "gRPC Listen Address")
	hostName := flag.String("hostname", os.Getenv("HOSTNAME"), "Hostname master can reach keeper with")
	keeperID := flag.String("keeper-id", os.Getenv("KEEPER_ID"), "Keeper ID")
	masterAddr := flag.String("master-addr", envOrDefault("MASTER_ADDR", "localhost:50051"), "Master gRPC Address")
	storageDir := flag.String("storage", envOrDefault("STORAGE_DIR", "./data"), "Storage directory")
	flag.Parse()
	if *keeperID == "" {
		log.Fatal("keeper-id is required")
	}
	if *hostName == "" {
		log.Fatal("hostname is required")
	}
	masterConn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial master: %v", err)
	}
	defer masterConn.Close()
	masterClient := masterpb.NewMasterInternalServiceClient(masterConn)
	storage := keeper.NewStorage(*storageDir)
	if err := storage.Init(); err != nil {
		log.Fatalf("initalizing storage: %v", err)
	}
	tcpHandler := keeper.NewTCPHandler(storage, *tcpPort, *keeperID, masterClient)
	keeperServer := keeper.NewKeeperServer(storage, masterClient, *keeperID)
	grpcServer := grpc.NewServer()
	keeperpb.RegisterKeeperServiceServer(grpcServer, keeperServer)
	go func() {
		lis, err := net.Listen("tcp", *grpcPort)
		if err != nil {
			log.Printf("gRPC server: %v", err)
		}
		log.Printf("gRPC server listening on %s", *grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server: %v", err)
		}
	}()
	go func() {
		failures := 0
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := masterClient.Heartbeat(ctx, &masterpb.HeartbeatRequest{
				KeeperId:   *keeperID,
				TcpAddress: *hostName,
			})
			cancel()
			if err != nil {
				log.Printf("heartbeat failed: %v", err)
				failures += 1
			} else {
				failures = 0
			}
			if failures >= 5 {
				log.Println("Could not reach master.\nShutting down...")
				tcpHandler.Stop()
				grpcServer.GracefulStop()
				os.Exit(0)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		for {
			files, err := storage.ListAllFiles()
			if err != nil {
				log.Printf("fetching files: %v\n[possible corruption in files]", err)
				time.Sleep(5 * time.Minute)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err = masterClient.FileSync(ctx, &masterpb.FileSyncRequest{
				KeeperId: *keeperID,
				Files:    files,
			})
			cancel()
			if err != nil {
				log.Printf("syncing files: %v", err)
			}
			time.Sleep(5 * time.Minute)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		tcpHandler.Stop()
		grpcServer.GracefulStop()
		os.Exit(0)
	}()
	log.Printf("TCP handler listening on port %s", *tcpPort)
	if err := tcpHandler.Start(); err != nil {
		log.Printf("TCP handler: %v", err)
	}
}

func envOrDefault(envVar, def string) string {
	if v := os.Getenv(envVar); v != "" {
		return v
	}
	return def
}
