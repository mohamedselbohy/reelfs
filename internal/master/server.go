// Package master contains all necessary implementations for master node functionality
package master

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"reelfs/gen/masterpb"

	"github.com/google/uuid"
)

type MasterTestServer struct {
	masterpb.UnimplementedMasterServiceServer
	lookup *LookupTable
}

type MasterInternalTestServer struct {
	masterpb.UnimplementedMasterInternalServiceServer
	lookup *LookupTable
}

func NewMasterInternalTestServer(lookup *LookupTable) *MasterInternalTestServer {
	return &MasterInternalTestServer{
		lookup: lookup,
	}
}

func NewMasterServer(lookup *LookupTable) *MasterTestServer {
	return &MasterTestServer{lookup: lookup}
}

func (s *MasterInternalTestServer) Heartbeat(ctx context.Context, req *masterpb.HeartbeatRequest) (*masterpb.HeartbeatResponse, error) {
	s.lookup.keepers.Store(req.KeeperId, &KeeperEntry{
		Hostname: req.TcpAddress,
		Address:  req.PublicAddress,
		LastSeen: time.Now(),
		IsAlive:  true,
	})
	return &masterpb.HeartbeatResponse{
		Success: true,
	}, nil
}

func (s *MasterInternalTestServer) FileSync(ctx context.Context, req *masterpb.FileSyncRequest) (*masterpb.FileSyncResponse, error) {
	return &masterpb.FileSyncResponse{
		Success: true,
	}, nil
}

func (s *MasterTestServer) LogLookup(ctx context.Context, req *masterpb.LogRequest) (*masterpb.LogResponse, error) {
	s.lookup.PrintLookup()
	return &masterpb.LogResponse{}, nil
}

func (s *MasterTestServer) RequestUpload(ctx context.Context, req *masterpb.UploadRequest) (*masterpb.UploadResponse, error) {
	keepersLen := 0
	var keepers []string
	s.lookup.keepers.Range(func(key any, value any) bool {
		val, ok := value.(*KeeperEntry)
		id, keyOk := key.(string)
		if !ok || val == nil || !keyOk {
			return true
		}
		if !val.IsAlive {
			return true
		}
		keepersLen += 1
		keepers = append(keepers, id)
		return true
	})
	if keepersLen == 0 {
		return &masterpb.UploadResponse{
			Success: false,
			Msg:     "no keepers to receive upload",
		}, nil
	}
	chosenKeeperIdx := (s.lookup.keepersIndex + 1) % keepersLen
	s.lookup.keepersIndex = chosenKeeperIdx
	newIdx := 0
	for newIdx < keepersLen {
		val, ok := s.lookup.keepers.Load(keepers[chosenKeeperIdx])
		chosenKeeperIdx = (chosenKeeperIdx + 1) % keepersLen
		newIdx += 1
		if !ok || val == nil {
			continue
		}
		chosenKeeper, ok := val.(*KeeperEntry)
		if !ok || chosenKeeper == nil {
			continue
		}
		transferID := uuid.NewString()
		transfer := TransferEntry{
			filename:   req.Filename,
			status:     TransferPending,
			commitTime: time.Now(),
		}
		s.lookup.transfers.Store(transferID, &transfer)
		return &masterpb.UploadResponse{
			Success:       true,
			TransferId:    transferID,
			KeeperAddress: chosenKeeper.Address,
		}, nil
	}

	return &masterpb.UploadResponse{
		Success: false,
		Msg:     "could not find a keeper for your upload try again later",
	}, nil
}

func (s *MasterInternalTestServer) NotifyUploadComplete(ctx context.Context, req *masterpb.UploadCompleteNotification) (*masterpb.UploadCompleteAck, error) {
	val, present := s.lookup.transfers.Load(req.TransferId)
	if !present {
		return &masterpb.UploadCompleteAck{
			Success: false,
		}, nil
	}
	transferEntry, transferOK := val.(*TransferEntry)
	if !transferOK || transferEntry == nil {
		s.lookup.transfers.Delete(req.TransferId)
		return &masterpb.UploadCompleteAck{
			Success: false,
		}, nil
	}
	if !req.Success {
		transferEntry.status = TransferFailed
		transferEntry.msg = req.Msg
		return &masterpb.UploadCompleteAck{
			Success: false,
		}, nil
	}
	go func() {
		s.lookup.keeperFiles.mu.RLock()
		s.lookup.fileKeepers.mu.RLock()
		val, _ = s.lookup.keeperFiles.records.LoadOrStore(req.KeeperId, &KeeperFilesEntry{
			mu:    sync.RWMutex{},
			files: make(map[string]struct{}),
		})
		keeperRecord, keeperOK := val.(*KeeperFilesEntry)
		if !keeperOK || keeperRecord == nil {
			keeperRecord = &KeeperFilesEntry{
				mu:    sync.RWMutex{},
				files: make(map[string]struct{}),
			}
			s.lookup.keeperFiles.records.Store(req.KeeperId, keeperRecord)
		}
		keeperRecord.mu.Lock()
		keeperRecord.files[transferEntry.filename] = struct{}{}
		val, _ = s.lookup.fileKeepers.records.LoadOrStore(transferEntry.filename, &FileKeepersEntry{
			mu:       sync.RWMutex{},
			filesize: uint64(req.Filesize),
			keepers:  []FileOnKeeperStats{},
		})
		fileRecord, fileOK := val.(*FileKeepersEntry)
		if !fileOK || fileRecord == nil {
			fileRecord = &FileKeepersEntry{
				mu:       sync.RWMutex{},
				filesize: uint64(req.Filesize),
				keepers:  []FileOnKeeperStats{},
			}
			s.lookup.fileKeepers.records.Store(transferEntry.filename, fileRecord)
		}
		fileRecord.mu.Lock()
		fileRecord.keepers = append(fileRecord.keepers, FileOnKeeperStats{
			keeperID: req.KeeperId,
			path:     req.Filepath,
		})
		fileRecord.mu.Unlock()
		keeperRecord.mu.Unlock()
		s.lookup.fileKeepers.mu.RUnlock()
		s.lookup.keeperFiles.mu.RUnlock()
		transferEntry.status = TransferComplete
	}()
	return &masterpb.UploadCompleteAck{
		Success: true,
	}, nil
}

func (s *MasterTestServer) ConfirmUpload(ctx context.Context, req *masterpb.ConfirmUploadRequest) (*masterpb.ConfirmUploadResponse, error) {
	val, exists := s.lookup.transfers.Load(req.TransferId)
	if !exists {
		return &masterpb.ConfirmUploadResponse{
			Status: masterpb.ConfirmUploadResponse_MISSING,
		}, nil
	}
	transferRecord, transferOk := val.(*TransferEntry)
	if !transferOk || transferRecord == nil {
		return &masterpb.ConfirmUploadResponse{
			Status: masterpb.ConfirmUploadResponse_MISSING,
		}, nil
	}
	switch transferRecord.status {
	case TransferComplete:
		s.lookup.transfers.Delete(req.TransferId)
		return &masterpb.ConfirmUploadResponse{Status: masterpb.ConfirmUploadResponse_SUCCESS}, nil
	case TransferPending:
		return &masterpb.ConfirmUploadResponse{Status: masterpb.ConfirmUploadResponse_PENDING}, nil
	default:
		s.lookup.transfers.Delete(req.TransferId)
		return &masterpb.ConfirmUploadResponse{Status: masterpb.ConfirmUploadResponse_FAILED, Msg: transferRecord.msg}, nil
	}
}

func (s *MasterInternalTestServer) NotifyReplicationComplete(ctx context.Context, req *masterpb.ReplicationCompleteNotification) (*masterpb.ReplicationCompleteAck, error) {
	replicationKey := fmt.Sprintf("%s_{%s}->{%s}", req.Filename, req.SourceKeeperId, req.DestinationKeeperId)
	log.Printf("replication key: %s", replicationKey)
	val, exists := s.lookup.replications.Load(replicationKey)
	if !exists {
		log.Print("replication key doesn't exist")
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
	replicationRecord, recordOK := val.(*ReplicationEntry)
	if !recordOK || replicationRecord == nil {
		s.lookup.replications.Delete(replicationKey)
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
	replicationRecord.mu.Lock()
	val, exists = s.lookup.replications.Load(replicationKey)
	if !exists {
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
	replicationRecord, recordOK = val.(*ReplicationEntry)
	if !recordOK || replicationRecord == nil {
		s.lookup.replications.Delete(replicationKey)
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
	if !req.Success {
		s.lookup.replications.Delete(replicationKey)
		replicationRecord.mu.Unlock()
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
	switch req.SenderId {
	case req.SourceKeeperId:
		replicationRecord.srcStatus.status = ReplicationComplete
		replicationRecord.mu.Unlock()
		return &masterpb.ReplicationCompleteAck{Success: true}, nil
	case req.DestinationKeeperId:
		s.lookup.replications.Delete(replicationKey)
		replicationRecord.mu.Unlock()
		go func() {
			s.lookup.keeperFiles.mu.RLock()
			s.lookup.fileKeepers.mu.RLock()
			keeperFilesEntry := &KeeperFilesEntry{
				mu:    sync.RWMutex{},
				files: make(map[string]struct{}),
			}
			keeperFilesEntry.mu.Lock()
			val, loaded := s.lookup.keeperFiles.records.LoadOrStore(req.DestinationKeeperId, keeperFilesEntry)
			keeperFilesEntry, _ = val.(*KeeperFilesEntry)
			if loaded {
				keeperFilesEntry.mu.Lock()
			}
			keeperFilesEntry.files[req.Filename] = struct{}{}
			fileKeepersEntry := &FileKeepersEntry{
				mu:      sync.RWMutex{},
				keepers: []FileOnKeeperStats{},
			}
			fileKeepersEntry.mu.Lock()
			val, loaded = s.lookup.fileKeepers.records.LoadOrStore(req.Filename, fileKeepersEntry)
			fileKeepersEntry, _ = val.(*FileKeepersEntry)
			if loaded {
				fileKeepersEntry.mu.Lock()
			}
			fileKeepersEntry.keepers = append(fileKeepersEntry.keepers, FileOnKeeperStats{
				keeperID: req.DestinationKeeperId,
				path:     req.DestinationFilepath,
			})
			fileKeepersEntry.mu.Unlock()
			keeperFilesEntry.mu.Unlock()
			s.lookup.fileKeepers.mu.RUnlock()
			s.lookup.keeperFiles.mu.RUnlock()
		}()
		return &masterpb.ReplicationCompleteAck{Success: true}, nil
	default:
		replicationRecord.mu.Unlock()
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
}
