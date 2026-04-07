package main

import (
	"context"
	"flag"
	"fmt"
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

func getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return ""
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			fmt.Println("could not read private ip address")
		}
	}()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

func main() {
	privateIP := getLocalIP()
	dataDir := flag.String("data-dir", "dataDir", "")
	keeperID := flag.String("keeper-id", "1", "")
	tcpAddr := flag.String("tcp-addr", "50050", "")
	grpcAddr := flag.String("grpc-addr", "50051", "")
	masterAddr := flag.String("master-addr", "localhost:50040", "")
	flag.Parse()
	log.Printf("keeper of id %s listening on port %s for directory %s", *keeperID, *tcpAddr, *dataDir)
	st := keeper.NewStorage(*dataDir)
	if err := st.Init(); err != nil {
		log.Fatal(err)
	}
	masterConn, err := grpc.NewClient(*masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal("could not connect to master")
	}
	masterClient := masterpb.NewMasterInternalServiceClient(masterConn)
	tcpHandler := keeper.NewTCPHandler(st, ":"+*tcpAddr, *keeperID, masterClient)
	grpcServer := grpc.NewServer()
	keeperServer := keeper.NewKeeperServer(st, masterClient, *keeperID)
	keeperpb.RegisterKeeperServiceServer(grpcServer, keeperServer)
	go func() {
		lis, err := net.Listen("tcp", ":"+*grpcAddr)
		if err != nil {
			log.Fatalf("connecting to grpc: %v", err)
		}
		log.Printf("listening to grpc on port %s", *grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("connecting to grpc: %v", err)
		}
	}()
	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, syscall.SIGTERM, syscall.SIGINT)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		failures := 0
		for {
			select {
			case <-time.After(1 * time.Second):
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, err := masterClient.Heartbeat(ctx, &masterpb.HeartbeatRequest{
					KeeperId:      *keeperID,
					TcpAddress:    privateIP + ":" + *grpcAddr,
					PublicAddress: privateIP + ":" + *tcpAddr,
				})
				cancel()
				if err != nil {
					if failures < 4 {
						failures += 1
						log.Printf("could not reach master, retrying...: %v", err)
					} else {
						log.Printf("could not reach master, terminating...: %v", err)
						grpcServer.GracefulStop()
						if err := tcpHandler.Stop(); err != nil {
							log.Fatalf("stopping server: %v", err)
						}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		<-sigch
		log.Print("shutting down server...")
		cancel()
		if err := tcpHandler.Stop(); err != nil {
			log.Fatalf("stopping server: %v", err)
		}
		grpcServer.GracefulStop()
	}()
	if err := tcpHandler.Start(); err != nil {
		log.Fatalf("starting server: %v", err)
	}
}
