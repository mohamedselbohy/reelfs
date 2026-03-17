// Package master contains all necessary implementations for master node functionality
package master

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"reelfs/gen/masterpb"

	"github.com/google/uuid"
)

type MasterInternalServer struct {
	masterpb.UnimplementedMasterInternalServiceServer
	lookup *LookupTable
}

type MasterServer struct {
	masterpb.UnimplementedMasterServiceServer
	lookup *LookupTable
}

func NewMasterServer(lookup *LookupTable) *MasterServer {
	return &MasterServer{lookup: lookup}
}

func NewMasterInternalServer(lookup *LookupTable) *MasterInternalServer {
	return &MasterInternalServer{lookup: lookup}
}

func (s *MasterInternalServer) Heartbeat(ctx context.Context, req *masterpb.HeartbeatRequest) (*masterpb.HeartbeatResponse, error) {
	s.lookup.keepers[req.KeeperId] = &KeeperStatus{
		Hostname: req.TcpAddress,
		Address:  req.PublicAddress,
		LastSeen: time.Now(),
	}
	return &masterpb.HeartbeatResponse{
		Success: true,
	}, nil
}

func (s *MasterInternalServer) notifyAlive(keeperID string) {
	v, ok := s.lookup.keepers[keeperID]
	if !ok {
		s.lookup.keepers[keeperID] = &KeeperStatus{}
	}
	v.LastSeen = time.Now()
}

func (s *MasterInternalServer) FileSync(ctx context.Context, req *masterpb.FileSyncRequest) (*masterpb.FileSyncResponse, error) {
	s.lookup.keeperFiles.mu.RLock()
	s.lookup.fileKeepers.mu.RLock()
	s.notifyAlive(req.KeeperId)
	_, exists := s.lookup.keeperFiles.entries[req.KeeperId]
	if !exists {
		s.lookup.keeperFiles.entries[req.KeeperId] = struct {
			mu    *sync.RWMutex
			files map[string]struct{}
		}{
			mu:    &sync.RWMutex{},
			files: make(map[string]struct{}),
		}
	}
	keeperEntry := s.lookup.keeperFiles.entries[req.KeeperId]
	lookupFiles := keeperEntry.files
	keeperEntry.mu.Lock()
	for _, v := range req.Files {
		if _, exists := lookupFiles[v.Filename]; !exists {
			s.lookup.keeperFiles.entries[req.KeeperId].files[v.Filename] = struct{}{}
			if _, exists = s.lookup.fileKeepers.entries[v.Filename]; !exists {
				s.lookup.fileKeepers.entries[v.Filename] = &FileEntry{
					mu: sync.RWMutex{},
					entry: InnerFileEntry{
						Keepers: []struct {
							KeeperID string
							Path     string
						}{},
					},
				}
			}
			s.lookup.fileKeepers.entries[v.Filename].mu.Lock()
			s.lookup.fileKeepers.entries[v.Filename].entry.Keepers = append(s.lookup.fileKeepers.entries[v.Filename].entry.Keepers, struct {
				KeeperID string
				Path     string
			}{KeeperID: req.KeeperId, Path: v.Filepath})
			s.lookup.fileKeepers.entries[v.Filename].mu.Unlock()
		}
	}
	keeperEntry.mu.Unlock()
	s.lookup.fileKeepers.mu.RUnlock()
	s.lookup.keeperFiles.mu.RUnlock()
	return &masterpb.FileSyncResponse{
		Success: true,
	}, nil
}

func (s *MasterServer) RequestUpload(ctx context.Context, req *masterpb.UploadRequest) (*masterpb.UploadResponse, error) {
	if len(s.lookup.keepers) == 0 {
		return &masterpb.UploadResponse{
			Success: false,
			Msg:     "no datakeepers available",
		}, fmt.Errorf("no datakeepers available")
	}
	var aliveKeepers []string
	for id, status := range s.lookup.keepers {
		if time.Since(status.LastSeen) < 3*time.Second {
			aliveKeepers = append(aliveKeepers, id)
		}
	}
	s.lookup.keeperIndex = (s.lookup.keeperIndex + 1) % len(aliveKeepers)
	selectedID := aliveKeepers[s.lookup.keeperIndex]
	selectedAddr := s.lookup.keepers[selectedID].Address
	transferID := uuid.NewString()
	s.lookup.transfers[transferID] = struct {
		filename   string
		status     TransferStatus
		msg        string
		commitTime time.Time
	}{
		filename:   req.Filename,
		status:     TransferPending,
		commitTime: time.Now(),
	}
	return &masterpb.UploadResponse{
		Success:       true,
		TransferId:    transferID,
		KeeperAddress: selectedAddr,
	}, nil
}

func (s *MasterInternalServer) NotifyUploadComplete(ctx context.Context, req *masterpb.UploadCompleteNotification) (*masterpb.UploadCompleteAck, error) {
	s.lookup.keeperFiles.mu.RLock()
	s.lookup.fileKeepers.mu.RLock()
	transferStatus, exists := s.lookup.transfers[req.TransferId]
	if !exists {
		s.lookup.fileKeepers.mu.RUnlock()
		s.lookup.keeperFiles.mu.RUnlock()
		return &masterpb.UploadCompleteAck{
			Success: false,
		}, fmt.Errorf("invalid transfer id")
	}
	if !req.Success {
		s.lookup.fileKeepers.mu.RUnlock()
		s.lookup.keeperFiles.mu.RUnlock()
		s.lookup.transfers[req.TransferId] = struct {
			filename   string
			status     TransferStatus
			msg        string
			commitTime time.Time
		}{
			filename:   transferStatus.filename,
			status:     TransferFailed,
			msg:        req.Msg,
			commitTime: transferStatus.commitTime,
		}
		return &masterpb.UploadCompleteAck{
			Success: false,
		}, fmt.Errorf("failure acknowledged")
	}
	_, exists = s.lookup.keeperFiles.entries[req.KeeperId]
	if !exists {
		s.lookup.keeperFiles.entries[req.KeeperId] = struct {
			mu    *sync.RWMutex
			files map[string]struct{}
		}{
			mu:    &sync.RWMutex{},
			files: make(map[string]struct{}),
		}
	}
	s.lookup.keeperFiles.entries[req.KeeperId].mu.Lock()
	s.lookup.keeperFiles.entries[req.KeeperId].files[transferStatus.filename] = struct{}{}
	s.lookup.keeperFiles.entries[req.KeeperId].mu.Unlock()
	s.lookup.fileKeepers.entries[transferStatus.filename] = &FileEntry{
		mu: sync.RWMutex{},
		entry: InnerFileEntry{
			Keepers: []struct {
				KeeperID string
				Path     string
			}{
				{
					KeeperID: req.KeeperId,
					Path:     req.Filepath,
				},
			},
		},
	}
	s.lookup.fileKeepers.mu.RUnlock()
	s.lookup.keeperFiles.mu.RUnlock()
	return &masterpb.UploadCompleteAck{
		Success: true,
	}, nil
}

func (s *MasterServer) ConfirmUpload(ctx context.Context, req *masterpb.ConfirmUploadRequest) (*masterpb.ConfirmUploadResponse, error) {
	transfer, exists := s.lookup.transfers[req.TransferId]
	if !exists {
		return &masterpb.ConfirmUploadResponse{
			Status: masterpb.ConfirmUploadResponse_MISSING,
		}, fmt.Errorf("transfer id doesn't exist")
	}
	switch transfer.status {
	case TransferPending:
		return &masterpb.ConfirmUploadResponse{Status: masterpb.ConfirmUploadResponse_PENDING}, nil
	case TransferSuccessful:
		delete(s.lookup.transfers, req.TransferId)
		return &masterpb.ConfirmUploadResponse{Status: masterpb.ConfirmUploadResponse_SUCCESS}, nil
	default:
		delete(s.lookup.transfers, req.TransferId)
		return &masterpb.ConfirmUploadResponse{Status: masterpb.ConfirmUploadResponse_FAILED, Msg: transfer.msg}, errors.New(transfer.msg)
	}
}

func (s *MasterInternalServer) NotifyReplicationComplete(ctx context.Context, req *masterpb.ReplicationCompleteNotification) (*masterpb.ReplicationCompleteAck, error) {
	replicationRecord, exists := s.lookup.replications[req.Filename]
	if !exists {
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
	replicationRecord.mu.Lock()
	defer replicationRecord.mu.Unlock()
	if replicationRecord, exists = s.lookup.replications[req.Filename]; !exists {
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
	switch req.SenderId {
	case req.SourceKeeperId:
		if !req.Success {
			delete(s.lookup.replications, req.Filename)
			return &masterpb.ReplicationCompleteAck{Success: false}, nil
		} else {
			return &masterpb.ReplicationCompleteAck{Success: true}, nil
		}
	case req.DestinationKeeperId:
		delete(s.lookup.replications, req.Filename)
		if !req.Success {
			return &masterpb.ReplicationCompleteAck{Success: false}, nil
		} else {
			s.lookup.keeperFiles.mu.RLock()
			s.lookup.fileKeepers.mu.RLock()
			_, exists := s.lookup.keeperFiles.entries[req.DestinationKeeperId]
			if !exists {
				s.lookup.keeperFiles.entries[req.DestinationKeeperId] = struct {
					mu    *sync.RWMutex
					files map[string]struct{}
				}{
					mu:    &sync.RWMutex{},
					files: make(map[string]struct{}),
				}
			}
			s.lookup.keeperFiles.entries[req.DestinationKeeperId].mu.Lock()
			keeperEntry := s.lookup.keeperFiles.entries[req.DestinationKeeperId]
			keeperEntry.files[req.Filename] = struct{}{}
			keeperEntry.mu.Unlock()
			_, exists = s.lookup.fileKeepers.entries[req.Filename]
			if !exists {
				s.lookup.fileKeepers.entries[req.Filename] = &FileEntry{
					mu: sync.RWMutex{},
					entry: InnerFileEntry{
						Keepers: []struct {
							KeeperID string
							Path     string
						}{},
					},
				}
			}
			s.lookup.fileKeepers.entries[req.Filename].mu.Lock()
			fileEntry := s.lookup.fileKeepers.entries[req.Filename]
			fileEntry.entry.Keepers = append(fileEntry.entry.Keepers, struct {
				KeeperID string
				Path     string
			}{KeeperID: req.DestinationKeeperId, Path: req.DestinationFilepath})
			fileEntry.mu.Unlock()
			s.lookup.fileKeepers.mu.RUnlock()
			s.lookup.keeperFiles.mu.RUnlock()
			return &masterpb.ReplicationCompleteAck{Success: true}, nil
		}
	default:
		return &masterpb.ReplicationCompleteAck{Success: false}, nil
	}
}
