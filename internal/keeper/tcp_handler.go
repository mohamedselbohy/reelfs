// Package keeper handles the keeper inner functionalities
package keeper

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"reelfs/gen/masterpb"
	"reelfs/pkg/protocol"
)

type TCPHandler struct {
	storage      *Storage
	tcpAddr      string
	listener     net.Listener
	masterClient masterpb.MasterInternalServiceClient
	keeperID     string
}

func NewTCPHandler(storage *Storage, tcpAddr string, keeperID string, masterClient masterpb.MasterInternalServiceClient) *TCPHandler {
	return &TCPHandler{
		storage:      storage,
		tcpAddr:      tcpAddr,
		keeperID:     keeperID,
		masterClient: masterClient,
	}
}

func (h *TCPHandler) Start() error {
	listener, err := net.Listen("tcp", h.tcpAddr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", h.tcpAddr, err)
	}
	h.listener = listener
	log.Printf("TCP Handler Listening on %s", h.tcpAddr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if err == net.ErrClosed {
				return fmt.Errorf("connection error: %v [terminating...]", err)
			}
			log.Printf("accept error: %v", err)
			continue
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
				Enable:   true,
				Idle:     5 * time.Minute,
				Interval: 1 * time.Minute,
				Count:    5,
			})
		}
		go h.HandleConnection(conn)
	}
}

func (h *TCPHandler) Stop() error {
	if h.listener != nil {
		return h.listener.Close()
	}
	return nil
}

func (h *TCPHandler) HandleConnection(conn net.Conn) {
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	header := make([]byte, protocol.HeaderSize)
	_, err := io.ReadFull(conn, header)
	if err != nil {
		h.SendError(conn, "reading header: %v", err)
		return
	}
	op, payloadSize, err := protocol.DecodeHeader(header)
	if err != nil {
		h.SendError(conn, "decoding header: %v", err)
		return
	}
	metadata := make([]byte, payloadSize)
	_, err = io.ReadFull(conn, metadata)
	if err != nil {
		h.SendError(conn, "reading metadata: %v", err)
		return
	}
	switch op {
	case protocol.OpUpload:
		h.HandleUpload(conn, metadata)
	case protocol.OpDownload:
		h.HandleDownload(conn, metadata)
	case protocol.OpReplicate:
		h.HandleReplicate(conn, metadata)
	default:
		h.SendError(conn, "unknown opcode: %d", op)
	}
}

func (h *TCPHandler) HandleUpload(conn net.Conn, metadata []byte) {
	transferID, filename, filesize, err := protocol.DecodeMetadataUpload(metadata)
	if err != nil {
		h.SendError(conn, "decoding upload metadata: %v", err)
		return
	}
	log.Printf("Receiving upload metadata %s (%d bytes)", filename, filesize)
	conn.SetReadDeadline(time.Time{})
	defer conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	limit := io.LimitReader(conn, int64(filesize))
	filePath, err := h.storage.StoreFile(filename, limit, filesize)
	if err != nil {
		h.SendError(conn, "storing file: %v", err)
		return
	}
	log.Printf("Upload_{%s} complete: %s -> %s", transferID, filename, filePath)
	h.SendOk(conn)
}

func (h *TCPHandler) HandleDownload(conn net.Conn, metadata []byte) {
	filename, offset, length, err := protocol.DecodeMetadataDownload(metadata)
	if err != nil {
		h.SendError(conn, "decoding download metadata: %v", err)
		return
	}
	conn.SetReadDeadline(time.Time{})

	log.Printf("Serving download: %s offset=%d length=%d", filename, offset, length)
	f, err := h.storage.OpenFile(filename)
	if err != nil {
		h.SendError(conn, "opening file: %v", err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		h.SendError(conn, "querying file size: %v", err)
		return
	}
	if int64(offset+length) > info.Size() {
		h.SendError(conn, "requested range (%d,%d) exceeds file size %d", offset, offset+length, info.Size())
		return
	}
	if offset > 0 {
		_, err = f.Seek(int64(offset), io.SeekStart)
		if err != nil {
			h.SendError(conn, "seeking file: %v", err)
			return
		}
	}
	h.SendOk(conn)
	n, err := io.CopyN(conn, f, int64(length))
	if err != nil && err != io.EOF {
		log.Printf("download error: %v", err)
		return
	}
	log.Printf("Download complete: %s send %d bytes", filename, n)
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
}

func (h *TCPHandler) HandleReplicate(conn net.Conn, metadata []byte) {
	filename, filesize, err := protocol.DecodeMetadataReplicate(metadata)
	if err != nil {
		h.SendError(conn, "decoding replicate metadata: %v", err)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		h.masterClient.NotifyReplicationComplete(ctx, &masterpb.ReplicationCompleteNotification{
			Success:             false,
			Filename:            filename,
			DestinationKeeperId: h.keeperID,
			DestinationFilepath: "",
		})
		cancel()
		return
	}
	conn.SetReadDeadline(time.Time{})

	log.Printf("Recieving replication: %s (%d bytes)", filename, filesize)
	limit := io.LimitReader(conn, int64(filesize))
	filePath, err := h.storage.StoreFile(filename, limit, filesize)
	if err != nil {
		h.SendError(conn, "storing replicated file: %v", err)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		h.masterClient.NotifyReplicationComplete(ctx, &masterpb.ReplicationCompleteNotification{
			Success:             false,
			Filename:            filename,
			DestinationKeeperId: h.keeperID,
			DestinationFilepath: "",
		})
		cancel()
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		return
	}
	log.Printf("Replication complete: %s -> %s", filename, filePath)
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	msgBytes := []byte(filePath)
	sentMetadata := make([]byte, 2+len(msgBytes))
	binary.BigEndian.PutUint16(sentMetadata[0:2], uint16(len(msgBytes)))
	copy(sentMetadata[2:2+len(msgBytes)], msgBytes)
	msg := protocol.BuildFullMessage(protocol.OpOk, sentMetadata)
	conn.Write(msg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h.masterClient.NotifyReplicationComplete(ctx, &masterpb.ReplicationCompleteNotification{
		Success:             true,
		Filename:            filename,
		DestinationKeeperId: h.keeperID,
		DestinationFilepath: filePath,
	})
	cancel()
}

func (h *TCPHandler) SendOk(conn net.Conn) {
	msg := protocol.BuildFullMessage(protocol.OpOk, nil)
	conn.Write(msg)
}

func (h *TCPHandler) SendError(conn net.Conn, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("TCP error: %s", msg)
	errorMsg := protocol.BuildFullMessage(protocol.OpError, protocol.EncodeMetadataError(msg))
	conn.Write(errorMsg)
}
