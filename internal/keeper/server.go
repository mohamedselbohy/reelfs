package keeper

import (
	"context"
	"fmt"
	"log"
	"os"

	"reelfs/gen/keeperpb"
	"reelfs/gen/masterpb"
	"reelfs/internal/client"
)

type KeeperServer struct {
	keeperpb.UnimplementedKeeperServiceServer
	storage      *Storage
	masterClient masterpb.MasterInternalServiceClient
	KeeperID     string
}

func NewKeeperServer(storage *Storage, masterClient masterpb.MasterInternalServiceClient, KeeperID string) *KeeperServer {
	return &KeeperServer{
		storage:      storage,
		masterClient: masterClient,
		KeeperID:     KeeperID,
	}
}

func (s *KeeperServer) Replicate(ctx context.Context, req *keeperpb.ReplicateCommand) (*keeperpb.ReplicateResponse, error) {
	f, err := os.Open(req.Filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return &keeperpb.ReplicateResponse{Success: false, Message: "file does not exist"}, err
		}
		return &keeperpb.ReplicateResponse{Success: false, Message: fmt.Sprintf("open file: %v", err)}, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return &keeperpb.ReplicateResponse{Success: false, Message: fmt.Sprintf("stat file: %v", err)}, err
	}
	filesize := uint64(info.Size())
	filename := req.Filename

	log.Printf("Replicating to keeper at addr: %s", req.DestinationAddress)
	tcpClient, err := client.NewTCPClient(req.DestinationAddress)
	if err != nil {
		return &keeperpb.ReplicateResponse{Success: false, Message: fmt.Sprintf("dial destination: %v", err)}, err
	}
	go s.doReplicate(ctx, req.DestinationId, filename, filesize, f, tcpClient)
	return &keeperpb.ReplicateResponse{Success: true, Message: "replication started"}, nil
}

func (s *KeeperServer) doReplicate(ctx context.Context, destinationID string, filename string, filesize uint64, f *os.File, tcpClient *client.TCPClient) {
	defer f.Close()
	defer tcpClient.Close()
	path, err := tcpClient.Replicate(filename, f, filesize)
	if err != nil {
		s.masterClient.NotifyReplicationComplete(ctx, &masterpb.ReplicationCompleteNotification{
			Filename:            filename,
			SourceKeeperId:      s.KeeperID,
			DestinationKeeperId: destinationID,
			DestinationFilepath: "",
			Success:             false,
		})
	} else {
		s.masterClient.NotifyReplicationComplete(ctx, &masterpb.ReplicationCompleteNotification{
			Filename:            filename,
			SourceKeeperId:      s.KeeperID,
			DestinationKeeperId: destinationID,
			DestinationFilepath: path,
			Success:             true,
		})
	}
}
