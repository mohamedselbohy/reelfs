// Package master contains all necessary implementations for master node functionality
package master

import "reelfs/gen/masterpb"

type MasterInternalServer struct {
	masterpb.UnimplementedMasterInternalServiceServer
}

type MasterServer struct {
	masterpb.UnimplementedMasterServiceServer
}

func NewMasterServer() *MasterServer {
	return &MasterServer{}
}

func NewMasterInternalServer() *MasterInternalServer {
	return &MasterInternalServer{}
}
