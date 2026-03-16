package keeper

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"reelfs/gen/shared"
)

var (
	ErrFileNotFound = errors.New("file not found")
	ErrInvalidPath  = errors.New("invalid file path")
)

type Storage struct {
	dataDir string
	mu      sync.RWMutex
}

func NewStorage(dataDir string) *Storage {
	return &Storage{
		dataDir: dataDir,
	}
}

func (s *Storage) Init() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return os.MkdirAll(s.dataDir, 0o755)
}

func (s *Storage) HashFileName(filename string) string {
	hash := sha256.Sum256([]byte(filename))
	return hex.EncodeToString(hash[:2])
}

func (s *Storage) GetFilePath(filename string) (string, error) {
	dirHash := s.HashFileName(filename)
	filePath := filepath.Join(s.dataDir, dirHash, filename)
	absBase, _ := filepath.Abs(s.dataDir)
	absFilePath, _ := filepath.Abs(filePath)
	if !strings.HasPrefix(absFilePath, absBase) {
		return "", ErrInvalidPath
	}
	return filePath, nil
}

func (s *Storage) StoreFile(filename string, data io.Reader, expectedSize uint64) (string, error) {
	s.mu.Lock()
	dirHash := s.HashFileName(filename)
	dirPath := filepath.Join(s.dataDir, dirHash)

	if err := os.MkdirAll(dirPath, 0o755); err != nil {
		s.mu.Unlock()
		return "", fmt.Errorf("creating directory: %w", err)
	}
	filePath := filepath.Join(dirPath, filename)
	absBase, _ := filepath.Abs(s.dataDir)
	absFilePath, _ := filepath.Abs(filePath)
	if !strings.HasPrefix(absFilePath, absBase) {
		s.mu.Unlock()
		return "", ErrInvalidPath
	}
	s.mu.Unlock()
	tmpFile, err := os.CreateTemp(dirPath, ".tmp-*")
	if err != nil {
		return "", fmt.Errorf("creating file: %v", err)
	}
	tmpPath := tmpFile.Name()
	written, err := io.Copy(tmpFile, data)
	tmpFile.Close()
	if err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("writing to file: %v", err)
	}
	if uint64(written) != expectedSize {
		os.Remove(tmpPath)
		return "", fmt.Errorf("expected %d bytes, wrote %d", expectedSize, written)
	}
	if err := os.Rename(tmpPath, filePath); err != nil {
		os.Remove(tmpPath)
		return "", fmt.Errorf("commiting file: %w", err)
	}
	return filePath, nil
}

func (s *Storage) fileExists(filePath string) bool {
	info, err := os.Stat(filePath)
	return err == nil && !info.IsDir()
}

func (s *Storage) FileExists(filename string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	filePath, err := s.GetFilePath(filename)
	if err != nil {
		return false
	}
	return s.fileExists(filePath)
}

func (s *Storage) OpenFile(filename string) (*os.File, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	filePath, err := s.GetFilePath(filename)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}
		return nil, fmt.Errorf("opening file: %w", err)
	}
	return f, nil
}

func (s *Storage) DeleteFile(filename string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	filePath, err := s.GetFilePath(filename)
	if err != nil {
		return err
	}
	if err := os.Remove(filePath); err != nil {
		return err
	}
	os.Remove(filepath.Dir(filePath))
	return nil
}

func (s *Storage) ListAllFiles() ([]*shared.FileInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var files []*shared.FileInfo
	err := filepath.Walk(s.dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		filename := filepath.Base(path)
		files = append(files, &shared.FileInfo{
			Filename: filename,
			Filepath: path,
			Filesize: info.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking storage directory: %v", err)
	}
	return files, nil
}
