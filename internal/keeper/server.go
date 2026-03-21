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
	// log.Printf("replicating %s -> %s", s.KeeperID, req.DestinationId)
	f, err := os.Open(req.Filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return &keeperpb.ReplicateResponse{Success: false, Msg: "file does not exist", InternalError: true}, err
		}
		return &keeperpb.ReplicateResponse{Success: false, Msg: fmt.Sprintf("open file: %v", err), InternalError: true}, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return &keeperpb.ReplicateResponse{Success: false, Msg: fmt.Sprintf("stat file: %v", err), InternalError: true}, err
	}
	filesize := uint64(info.Size())
	filename := req.Filename

	log.Printf("Replicating to keeper at addr: %s", req.DestinationAddress)
	tcpClient, err := client.NewTCPClient(req.DestinationAddress)
	log.Printf("replicating to %s", req.DestinationAddress)
	if err != nil {
		f.Close()
		return &keeperpb.ReplicateResponse{Success: false, Msg: fmt.Sprintf("dial destination: %v", err), InternalError: false}, err
	}
	go s.doReplicate(context.Background(), req.DestinationId, filename, filesize, f, tcpClient)
	return &keeperpb.ReplicateResponse{Success: true, Msg: "replication started"}, nil
}

func (s *KeeperServer) doReplicate(ctx context.Context, destinationID string, filename string, filesize uint64, f *os.File, tcpClient *client.TCPClient) {
	defer f.Close()
	defer tcpClient.Close()
	path, err := tcpClient.Replicate(s.KeeperID, filename, f, filesize)
	if err != nil {
		s.masterClient.NotifyReplicationComplete(ctx, &masterpb.ReplicationCompleteNotification{
			Filename:            filename,
			SourceKeeperId:      s.KeeperID,
			DestinationKeeperId: destinationID,
			DestinationFilepath: "",
			SenderId:            s.KeeperID,
			Success:             false,
		})
	} else {
		s.masterClient.NotifyReplicationComplete(ctx, &masterpb.ReplicationCompleteNotification{
			Filename:            filename,
			SourceKeeperId:      s.KeeperID,
			DestinationKeeperId: destinationID,
			DestinationFilepath: path,
			SenderId:            s.KeeperID,
			Success:             true,
		})
	}
}
