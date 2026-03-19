// Package client acts as a tcp client for reelfs keepers
package client

import (
	"fmt"
	"io"
	"net"
	"time"

	"reelfs/pkg/protocol"
)

type TCPClient struct {
	addr string
	conn net.Conn
}

func NewTCPClient(addr string) (*TCPClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAliveConfig(net.KeepAliveConfig{
			Enable:   true,
			Idle:     5 * time.Minute,
			Interval: 1 * time.Minute,
			Count:    5,
		})
	}

	return &TCPClient{
		addr,
		conn,
	}, nil
}

func (c *TCPClient) Close() {
	c.conn.Close()
}

func (c *TCPClient) Upload(transferID string, filename string, content io.Reader, filesize uint64) error {
	metadata := protocol.EncodeMetadataUpload(transferID, filename, filesize)
	fullMsg := protocol.BuildFullMessage(protocol.OpUpload, metadata)
	if _, err := c.conn.Write(fullMsg); err != nil {
		return err
	}
	if _, err := io.Copy(c.conn, content); err != nil {
		return err
	}
	header := make([]byte, protocol.HeaderSize)
	if _, err := c.conn.Read(header); err != nil {
		return err
	}
	op, metadataSize, err := protocol.DecodeHeader(header)
	if err != nil {
		return err
	}
	if op == protocol.OpError {
		errorMetadata := make([]byte, metadataSize)
		if _, err := c.conn.Read(errorMetadata); err != nil {
			return err
		}
		errorMsg, err := protocol.DecodeMetadataError(errorMetadata)
		if err != nil {
			return err
		}
		return fmt.Errorf("[keeper error]: %s", errorMsg)
	} else if op != protocol.OpOk {
		return fmt.Errorf("invalid response")
	}
	return nil
}

func (c *TCPClient) Replicate(srcID string, filename string, content io.Reader, filesize uint64) (string, error) {
	metadata := protocol.EncodeMetadataReplicate(filename, filesize, srcID)
	msg := protocol.BuildFullMessage(protocol.OpReplicate, metadata)
	if _, err := c.conn.Write(msg); err != nil {
		return "", err
	}
	if _, err := io.Copy(c.conn, content); err != nil {
		return "", err
	}
	header := make([]byte, protocol.HeaderSize)
	if _, err := c.conn.Read(header); err != nil {
		return "", err
	}
	op, metadataSize, err := protocol.DecodeHeader(header)
	if err != nil {
		return "", err
	}
	if op == protocol.OpError {
		errorMetadata := make([]byte, metadataSize)
		if _, err := c.conn.Read(errorMetadata); err != nil {
			return "", err
		}
		errorMsg, err := protocol.DecodeMetadataError(errorMetadata)
		if err != nil {
			return "", err
		}
		return "", fmt.Errorf("replication failed: %s", errorMsg)
	} else if op != protocol.OpOk {
		return "", fmt.Errorf("invalid response")
	}
	okMetadata := make([]byte, metadataSize)
	if _, err := c.conn.Read(okMetadata); err != nil {
		return "", err
	}
	path, err := protocol.DecodeMetadataError(okMetadata)
	if err != nil {
		return "", err
	}
	return path, nil
}

func (c *TCPClient) Download(filename string, offset uint64, length uint64) (io.Reader, error) {
	metadata := protocol.EncodeMetadataDownload(filename, offset, length)
	msg := protocol.BuildFullMessage(protocol.OpDownload, metadata)
	if _, err := c.conn.Write(msg); err != nil {
		return nil, err
	}
	header := make([]byte, protocol.HeaderSize)
	if _, err := c.conn.Read(header); err != nil {
		return nil, err
	}
	op, metadataSize, err := protocol.DecodeHeader(header)
	if err != nil {
		return nil, err
	}
	if op == protocol.OpError {
		errorMetadata := make([]byte, metadataSize)
		if _, err := c.conn.Read(errorMetadata); err != nil {
			return nil, err
		}
		errorMsg, err := protocol.DecodeMetadataError(errorMetadata)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("download failed: %s", errorMsg)
	} else if op != protocol.OpOk {
		return nil, fmt.Errorf("invalid response")
	}
	return io.LimitReader(c.conn, int64(length)), nil
}
