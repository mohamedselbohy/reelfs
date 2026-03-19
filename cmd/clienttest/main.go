package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"reelfs/gen/masterpb"
	"reelfs/internal/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// tcpAddr := flag.String("keeper-tcp", "50050", "")
	masterPort := flag.String("master-grpc", "50000", "")
	flag.Parse()
	masterAddr := ":" + *masterPort
	masterConn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connecting to master: %v", err)
	}
	masterClient := masterpb.NewMasterServiceClient(masterConn)
	f, err := os.Open("./clientDir/reel.mp4")
	if err != nil {
		log.Fatalf("opening file: %v", err)
	}
	stat, err := os.Stat("./clientDir/reel.mp4")
	if err != nil {
		log.Fatalf("getting file size: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	res, err := masterClient.RequestUpload(ctx, &masterpb.UploadRequest{
		Filename: "reel.mp4",
		Filesize: stat.Size(),
	})
	cancel()
	if err != nil {
		log.Fatalf("requesting upload: %v", err)
	}
	if !res.Success {
		log.Printf("upload request insuccessful: %s", res.Msg)
	} else {
		log.Printf("upload file to %s with TransferId: %s", res.KeeperAddress, res.TransferId)
	}

	client, err := client.NewTCPClient(res.KeeperAddress)
	if err != nil {
		log.Fatalf("starting tcp client: %v", err)
	}

	client.Upload(res.TransferId, "reel.mp4", f, uint64(stat.Size()))
	count := 0
outer:
	for count < 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := masterClient.ConfirmUpload(ctx, &masterpb.ConfirmUploadRequest{
			TransferId: res.TransferId,
		})
		cancel()
		if err != nil {
			log.Fatalf("upload confirmation: %v", err)
		}
		switch res.Status {
		case masterpb.ConfirmUploadResponse_PENDING:
			log.Printf("upload isn't registered yet. trying again in %d seconds...", 1<<count)
			time.Sleep((1 << count) * time.Second)
			log.Print("trying again now...")
			count += 1
		case masterpb.ConfirmUploadResponse_MISSING:
			log.Printf("this upload does not exist")
			break outer
		case masterpb.ConfirmUploadResponse_FAILED:
			log.Printf("upload went wrong: %s", res.Msg)
			break outer
		default:
			log.Print("File uploaded successfully")
			break outer
		}
	}
}
