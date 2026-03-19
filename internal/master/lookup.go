package master

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"reelfs/gen/keeperpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type KeeperEntry struct {
	Hostname string
	Address  string
	LastSeen time.Time
	IsAlive  bool
}

type KeeperFilesEntry struct {
	mu    sync.RWMutex
	files map[string]struct{} // set of strings
}

type KeeperFilesTable struct {
	mu      sync.RWMutex
	records sync.Map // Map[string]KeeperFilesEntry
}

type FileOnKeeperStats struct {
	keeperID string
	path     string
}

type FileKeepersEntry struct {
	mu       sync.RWMutex
	filesize uint64
	keepers  []FileOnKeeperStats
}

type FileKeepersTable struct {
	mu      sync.RWMutex
	records sync.Map
}

type TransferStatus int

const (
	TransferPending = iota
	TransferComplete
	TransferFailed
)

type TransferEntry struct {
	filename   string
	status     TransferStatus
	msg        string
	commitTime time.Time
}

type ReplicationStatus int

const (
	ReplicationPending ReplicationStatus = iota
	ReplicationComplete
	ReplicationFailed
)

type KeeperReplicationStatus struct {
	keeperID string
	status   ReplicationStatus
}

type ReplicationEntry struct {
	mu         sync.RWMutex
	srcStatus  KeeperReplicationStatus
	destStatus KeeperReplicationStatus
	startTime  time.Time
}

type LookupTable struct {
	keepersIndex      int
	replicationFactor int
	keepers           sync.Map
	keeperFiles       KeeperFilesTable
	fileKeepers       FileKeepersTable
	transfers         sync.Map
	replications      sync.Map
}

func NewLookupTable(replicationFactor int) *LookupTable {
	return &LookupTable{
		keepersIndex:      0,
		replicationFactor: replicationFactor,
		keeperFiles: KeeperFilesTable{
			mu: sync.RWMutex{},
		},
		fileKeepers: FileKeepersTable{
			mu: sync.RWMutex{},
		},
	}
}

func (t *TransferStatus) Print() string {
	switch *t {
	case TransferPending:
		return "Pending"
	case TransferComplete:
		return "Complete"
	default:
		return "Failed"
	}
}

func (r *ReplicationStatus) Print() string {
	switch *r {
	case ReplicationPending:
		return "Pending"
	case ReplicationComplete:
		return "Complete"
	default:
		return "Failed"
	}
}

func (l *LookupTable) PrintTransfers() {
	log.Print("Transfers: [id] [filename] [status] [commit time]")
	l.transfers.Range(func(key any, value any) bool {
		transferID, idOK := key.(string)
		transfer, transferOK := value.(*TransferEntry)
		if !idOK || !transferOK || transferID == "" || transfer == nil {
			return true
		}
		log.Printf("%s\t%s\t%s\t%s", transferID, transfer.filename, transfer.status.Print(), transfer.commitTime.Format(time.DateTime))
		return true
	})
}

// func (l *LookupTable) AddToKeeperFiles(id string, filename string) {
// 	keeperFilesRecord, _ := l.keeperFiles.records.LoadOrStore(id, &KeeperFilesEntry{
// 		mu:    sync.RWMutex{},
// 		files: make(map[string]struct{}),
// 	})
// 	record, recordOK := keeperFilesRecord.(*KeeperFilesEntry)
// 	if !recordOK || record == nil {
// 		return
// 	}
// 	record.mu.Lock()
// 	record.files[filename] = struct{}{}
// 	record.mu.Unlock()
// }
//
// func (l *LookupTable) AddToFileKeepers(filename string, keeperID string, filesize uint64, path string) {
// 	filesKeepersRecord, _ := l.fileKeepers.records.LoadOrStore(filename, &FileKeepersEntry{
// 		mu:       sync.RWMutex{},
// 		filesize: filesize,
// 		keepers:  []FileOnKeeperStats{},
// 	})
// 	record, recordOK := filesKeepersRecord.(*FileKeepersEntry)
// 	if !recordOK || record == nil {
// 		return
// 	}
// 	record.mu.Lock()
// 	record.keepers = append(record.keepers, FileOnKeeperStats{
// 		keeperID: keeperID,
// 		path:     path,
// 	})
// 	record.mu.Unlock()
// }

func (l *LookupTable) RemoveDeadKeepers() {
	// var nulls []any
	l.keepers.Range(func(key any, value any) bool {
		_, idOK := key.(string)
		keeperEntry, keeperOK := value.(*KeeperEntry)
		if !idOK || !keeperOK || keeperEntry == nil {
			// nulls = append(nulls, key)
			return true
		}
		if time.Since(keeperEntry.LastSeen) > 10*time.Second {
			keeperEntry.IsAlive = false
		}
		return true
	})
	// for id := range nulls {
	// 	l.keepers.CompareAndDelete(id, nil)
	// }
}

func (l *LookupTable) ReplicateFiles() {
	l.fileKeepers.mu.RLock()
	// var nulls []any
	l.fileKeepers.records.Range(func(key any, value any) bool {
		filename, nameOK := key.(string)
		keepersEntry, keepersOK := value.(*FileKeepersEntry)
		if !nameOK || !keepersOK || keepersEntry == nil {
			// nulls = append(nulls, key)
			return true
		}
		keepersEntry.mu.RLock()
		var potentialSendersWithPath []FileOnKeeperStats
		for _, keeper := range keepersEntry.keepers {
			val, exists := l.keepers.Load(keeper.keeperID)
			if !exists {
				continue
			}
			keeperEntry, keeperOK := val.(*KeeperEntry)
			if !keeperOK || keeperEntry == nil {
				continue
			}
			if !keeperEntry.IsAlive {
				continue
			}
			potentialSendersWithPath = append(potentialSendersWithPath, keeper)
		}
		neededReplicas := l.replicationFactor - len(potentialSendersWithPath)
		if neededReplicas <= 0 {
			keepersEntry.mu.RUnlock()
			return true
		}
		var potentialReceivers []string
		l.keepers.Range(func(key any, value any) bool {
			keeperID, idOK := key.(string)
			keeperEntry, keeperOK := value.(*KeeperEntry)
			if !idOK || !keeperOK || keeperEntry == nil {
				return true
			}
			isOwner := false
			for _, owner := range keepersEntry.keepers {
				if keeperID == owner.keeperID {
					isOwner = true
					break
				}
			}
			if !isOwner {
				potentialReceivers = append(potentialReceivers, keeperID)
			}
			return true
		})
		replicationsTBD := min(len(potentialReceivers), neededReplicas)
		rand.Shuffle(len(potentialReceivers), func(i int, j int) {
			potentialReceivers[i], potentialReceivers[j] = potentialReceivers[j], potentialReceivers[i]
		})
		replicationsDone := 0
		rand.Shuffle(len(potentialSendersWithPath), func(i int, j int) {
			potentialSendersWithPath[i], potentialSendersWithPath[j] = potentialSendersWithPath[j], potentialSendersWithPath[i]
		})
		startingSenderIndex := 0
		for receiverIndex := range len(potentialReceivers) {
			if replicationsDone >= replicationsTBD {
				break
			}
			successful := false
			actualSenderIndex := startingSenderIndex
			log.Printf("Trying to replicate file %s (%d Bytes) to keeper %s", filename, keepersEntry.filesize, potentialReceivers[receiverIndex])
			for range len(potentialSendersWithPath) {
				_, exists := l.replications.Load(fmt.Sprintf("%s_{%s}->{%s}", filename, potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex]))
				if exists {
					log.Printf("replication %s->%s already pending", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
					continue
				}
				log.Printf("\tTrying replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
				val, exists := l.keepers.Load(potentialSendersWithPath[actualSenderIndex].keeperID)
				if !exists {
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tSource not available")
					actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
					continue
				}
				keeper, keeperOK := val.(*KeeperEntry)
				if !keeperOK || keeper == nil {
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tSource not available")
					actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
					continue
				}
				if !keeper.IsAlive {
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tSource not available")
					actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
					continue
				}
				keeperConn, err := grpc.NewClient(keeper.Hostname, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tSource not available")
					actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
					continue
				}
				val, exists = l.keepers.Load(potentialReceivers[receiverIndex])
				if !exists {
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tDestination not available")
					break
				}
				destKeeperEntry, destOK := val.(*KeeperEntry)
				if !destOK || destKeeperEntry == nil {
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tDestination not available")
					break
				}
				if !destKeeperEntry.IsAlive {
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tDestination not available")
					break
				}
				keeperClient := keeperpb.NewKeeperServiceClient(keeperConn)
				replicationKey := fmt.Sprintf("%s_{%s}->{%s}", filename, potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
				replicationEntry := &ReplicationEntry{
					mu: sync.RWMutex{},
					srcStatus: KeeperReplicationStatus{
						keeperID: potentialSendersWithPath[actualSenderIndex].keeperID,
						status:   ReplicationPending,
					},
					destStatus: KeeperReplicationStatus{
						keeperID: potentialReceivers[receiverIndex],
						status:   ReplicationPending,
					},
				}
				replicationEntry.mu.Lock()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				res, err := keeperClient.Replicate(ctx, &keeperpb.ReplicateCommand{
					Filename:           filename,
					Filepath:           potentialSendersWithPath[actualSenderIndex].path,
					DestinationId:      potentialReceivers[receiverIndex],
					DestinationAddress: destKeeperEntry.Address,
				})
				cancel()
				if err != nil {
					replicationEntry.mu.Unlock()
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					log.Print("\tSource not available")
					actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
					continue
				}
				if !res.Success {
					replicationEntry.mu.Unlock()
					log.Printf("\tFailed replication %s->%s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex])
					if res.InternalError {
						log.Printf("\tSource failed, Source: %s", res.Msg)
						actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
						continue
					} else {
						log.Printf("\tSource failed to reach destination, Source: %s", res.Msg)
						break
					}
				}
				l.replications.Store(replicationKey, replicationEntry)
				replicationEntry.startTime = time.Now()
				replicationEntry.mu.Unlock()
				log.Printf("replication %s->%s started, source: %s", potentialSendersWithPath[actualSenderIndex].keeperID, potentialReceivers[receiverIndex], res.Msg)
				actualSenderIndex = (actualSenderIndex + 1) % len(potentialSendersWithPath)
				successful = true
				break
			}
			startingSenderIndex = actualSenderIndex
			if successful {
				replicationsDone += 1
			}
		}
		keepersEntry.mu.RUnlock()
		return true
	})
	l.fileKeepers.mu.RUnlock()
}

func (l *LookupTable) PrintLookup() {
	l.keeperFiles.mu.RLock()
	l.fileKeepers.mu.RLock()
	log.Print("\n\n\n")
	log.Print("Keepers: [Hostname] [Public Address] [Last Seen] [Is Alive?]")
	l.keepers.Range(func(key any, value any) bool {
		keeperID, idOK := key.(string)
		keeper, keeperOK := value.(*KeeperEntry)
		if !idOK || !keeperOK || keeper == nil {
			return true
		}
		liveliness := ""
		if keeper.IsAlive {
			liveliness = "Alive"
		} else {
			liveliness = "Dead"
		}
		log.Printf("[%s]:\t%s\t%s\t%s\t%s", keeperID, keeper.Hostname, keeper.Address, keeper.LastSeen.Format(time.DateTime), liveliness)
		return true
	})
	log.Print("\n")
	log.Print("KeeperFiles: [files...]")
	l.keeperFiles.records.Range(func(key any, value any) bool {
		keeperID, idOK := key.(string)
		files, filesOK := value.(*KeeperFilesEntry)
		if !idOK || !filesOK || files == nil {
			return true
		}
		line := fmt.Sprintf("[%s]:\t", keeperID)
		count := 0
		for file := range files.files {
			line += file
			if count < len(files.files)-1 {
				line += ", "
			}
			count += 1
		}
		log.Println(line)
		return true
	})
	log.Print("\n")
	log.Print("FileKeepers([filesize] Bytes): {[Keeper]: [Path]}...")
	l.fileKeepers.records.Range(func(key any, value any) bool {
		filename, nameOK := key.(string)
		fileEntry, entryOK := value.(*FileKeepersEntry)
		if !nameOK || !entryOK || fileEntry == nil {
			return true
		}
		line := fmt.Sprintf("[%s](%d Bytes): ", filename, fileEntry.filesize)
		for i, keeperStat := range fileEntry.keepers {
			line += fmt.Sprintf("{[%s]: \"%s\" }", keeperStat.keeperID, keeperStat.path)
			if i < len(fileEntry.keepers)-1 {
				line += ", "
			}
		}
		log.Println(line)
		return true
	})
	l.fileKeepers.mu.RUnlock()
	l.keeperFiles.mu.RUnlock()
	log.Print("\n")
	l.PrintTransfers()
	log.Print("\n")
	log.Print("Replications: {[src Keeper]: (Complete/Failed/Pending)} {[dest Keeper]: (Complete/Failed/Pending)} [startTime]")
	l.replications.Range(func(key any, value any) bool {
		replicationKey, keyOK := key.(string)
		replicationEntry, entryOK := value.(*ReplicationEntry)
		if !keyOK || !entryOK || replicationEntry == nil {
			return true
		}
		line := fmt.Sprintf("[%s]: ", replicationKey)
		line += fmt.Sprintf("src:{%s: %s}\tdest:{%s:%s}\t%s", replicationEntry.srcStatus.keeperID, replicationEntry.srcStatus.status.Print(), replicationEntry.destStatus.keeperID, replicationEntry.destStatus.status.Print(), replicationEntry.startTime.Format(time.DateTime))
		log.Print(line)
		return true
	})
}
