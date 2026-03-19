// Package protocol handles TCP header encoding and metadata for file handling and so on
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var ErrInvalidHeader = errors.New("invalid header")

const (
	OpUpload        = 0x01
	OpDownload      = 0x02
	OpReplicate     = 0x03
	OpCheckDownload = 0x04
	OpOk            = 0x10
	OpError         = 0x11
)

const HeaderSize = 9 // 1 -> OpCode, 8-> payloadsize (uint64)

func EncodeHeader(op byte, payloadSize uint64) []byte {
	header := make([]byte, HeaderSize)
	header[0] = op
	binary.BigEndian.PutUint64(header[1:], payloadSize)
	return header
}

func DecodeHeader(header []byte) (byte, uint64, error) {
	if len(header) != HeaderSize {
		return 0, 0, fmt.Errorf("%w: Expected %d, got %d", ErrInvalidHeader, HeaderSize, len(header))
	}
	op := header[0]
	payloadSize := binary.BigEndian.Uint64(header[1:])
	if payloadSize > (1 << 20) {
		return 0, 0, fmt.Errorf("%w: Payload too large, Maximum 1MB", ErrInvalidHeader)
	}
	return op, payloadSize, nil
}

func EncodeMetadataUpload(transferID string, filename string, filesize uint64) []byte {
	transferIDBytes := []byte(transferID)
	filenameBytes := []byte(filename)
	metadata := make([]byte, len(transferIDBytes)+len(filenameBytes)+12)
	binary.BigEndian.PutUint16(metadata[0:2], uint16(len(transferIDBytes)))
	copy(metadata[2:2+len(transferIDBytes)], transferIDBytes)
	binary.BigEndian.PutUint16(metadata[2+len(transferIDBytes):4+len(transferIDBytes)], uint16(len(filenameBytes)))
	copy(metadata[4+len(transferIDBytes):4+len(transferIDBytes)+len(filenameBytes)], filenameBytes)
	binary.BigEndian.PutUint64(metadata[4+len(transferIDBytes)+len(filenameBytes):], filesize)
	return metadata
}

func DecodeMetadataUpload(metadata []byte) (string, string, uint64, error) {
	if len(metadata) < 2 {
		return "", "", 0, fmt.Errorf("upload metadata too short")
	}
	transferIDSize := binary.BigEndian.Uint16(metadata[0:2])
	if len(metadata) < (int(transferIDSize) + 4) {
		return "", "", 0, fmt.Errorf("upload metadata too short")
	}
	transferID := string(metadata[2 : 2+transferIDSize])
	nameSize := binary.BigEndian.Uint16(metadata[2+transferIDSize : 4+transferIDSize])
	if len(metadata) < (12 + int(transferIDSize) + int(nameSize)) {
		return "", "", 0, fmt.Errorf("upload metadata too short for filename + filesize")
	}
	name := string(metadata[4+transferIDSize : 4+transferIDSize+nameSize])
	fileSize := binary.BigEndian.Uint64(metadata[4+transferIDSize+nameSize : 12+transferIDSize+nameSize])
	return transferID, name, fileSize, nil
}

func EncodeMetadataDownload(filename string, offset uint64, length uint64) []byte {
	filenameBytes := []byte(filename)
	metadata := make([]byte, 2+len(filenameBytes)+16)
	binary.BigEndian.PutUint16(metadata[0:2], uint16(len(filenameBytes)))
	copy(metadata[2:2+len(filenameBytes)], filenameBytes)
	binary.BigEndian.PutUint64(metadata[2+len(filenameBytes):10+len(filenameBytes)], offset)
	binary.BigEndian.PutUint64(metadata[10+len(filenameBytes):18+len(filenameBytes)], length)
	return metadata
}

func DecodeMetadataDownload(metadata []byte) (string, uint64, uint64, error) {
	if len(metadata) < 2 {
		return "", 0, 0, fmt.Errorf("download metadata too short")
	}
	nameSize := binary.BigEndian.Uint16(metadata[0:2])
	if len(metadata) < int(nameSize)+18 {
		return "", 0, 0, fmt.Errorf("download metadata too short")
	}
	name := string(metadata[2 : 2+nameSize])
	offset := binary.BigEndian.Uint64(metadata[2+nameSize : 10+nameSize])
	length := binary.BigEndian.Uint64(metadata[10+nameSize:])
	return name, offset, length, nil
}

func EncodeMetadataCheckDownload(filename string, filesize uint64) []byte {
	filenameBytes := []byte(filename)
	metadata := make([]byte, 10+len(filenameBytes))
	binary.BigEndian.PutUint16(metadata[0:2], uint16(len(filenameBytes)))
	copy(metadata[2:2+len(filenameBytes)], filenameBytes)
	binary.BigEndian.PutUint64(metadata[2+len(filenameBytes):10+len(filenameBytes)], filesize)
	return metadata
}

func DecodeMetadataCheckDownload(metadata []byte) (string, uint64, error) {
	if len(metadata) < 2 {
		return "", 0, errors.New("error metadata too short")
	}
	nameSize := binary.BigEndian.Uint16(metadata[0:2])
	if len(metadata) < 10+int(nameSize) {
		return "", 0, errors.New("error metadata too short")
	}
	filename := string(metadata[2 : 2+nameSize])
	filesize := binary.BigEndian.Uint64(metadata[2+nameSize : 10+nameSize])
	return filename, filesize, nil
}

func EncodeMetadataReplicate(filename string, filesize uint64, senderID string) []byte {
	// return EncodeMetadataUpload(filename, filesize)
	senderBytes := []byte(senderID)
	filenameBytes := []byte(filename)
	metadata := make([]byte, len(filenameBytes)+len(senderBytes)+12)
	binary.BigEndian.PutUint16(metadata[0:2], uint16(len(senderBytes)))
	copy(metadata[2:2+len(senderBytes)], senderBytes)
	binary.BigEndian.PutUint16(metadata[2+len(senderBytes):4+len(senderBytes)], uint16(len(filenameBytes)))
	copy(metadata[4+len(senderBytes):4+len(senderBytes)+len(filenameBytes)], filenameBytes)
	binary.BigEndian.PutUint64(metadata[4+len(senderBytes)+len(filenameBytes):12+len(senderBytes)+len(filenameBytes)], filesize)
	return metadata
}

func DecodeMetadataReplicate(metadata []byte) (string, string, uint64, error) {
	// return DecodeMetadataUpload(metadata)
	if len(metadata) < 2 {
		return "", "", 0, fmt.Errorf("replicate metadata too short")
	}
	senderSize := binary.BigEndian.Uint16(metadata[0:2])
	if len(metadata) < 10+int(senderSize) {
		return "", "", 0, fmt.Errorf("replicate metadata too short")
	}
	senderID := string(metadata[2 : 2+senderSize])
	nameSize := binary.BigEndian.Uint16(metadata[2+senderSize : 4+senderSize])
	if len(metadata) < 12+int(nameSize)+int(senderSize) {
		return "", "", 0, fmt.Errorf("replicate metadata too short")
	}
	filename := string(metadata[4+senderSize : 4+senderSize+nameSize])
	filesize := binary.BigEndian.Uint64(metadata[4+senderSize+nameSize : 12+senderSize+nameSize])
	return senderID, filename, filesize, nil
}

func EncodeMetadataError(message string) []byte {
	messageBytes := []byte(message)
	metadata := make([]byte, 2+len(messageBytes))
	binary.BigEndian.PutUint16(metadata[0:2], uint16(len(messageBytes)))
	copy(metadata[2:], messageBytes)
	return metadata
}

func DecodeMetadataError(metadata []byte) (string, error) {
	if len(metadata) < 2 {
		return "", fmt.Errorf("error metadata too short")
	}
	messageSize := binary.BigEndian.Uint16(metadata[0:2])
	if len(metadata) < 2+int(messageSize) {
		return "", fmt.Errorf("error metadata too short")
	}
	message := string(metadata[2 : 2+messageSize])
	return message, nil
}

func BuildFullMessage(op byte, metadata []byte) []byte {
	metadataSize := uint64(len(metadata))
	header := EncodeHeader(op, metadataSize)
	result := make([]byte, HeaderSize+int(metadataSize))
	copy(result[0:HeaderSize], header)
	if metadataSize > 0 {
		copy(result[HeaderSize:], metadata)
	}
	return result
}
