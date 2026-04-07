# ReelFS

A distributed file system for MP4 files with built-in replication for fault tolerance and high availability. ReelFS provides an eventually consistent storage layer that automatically replicates files across multiple data keepers.

## Architecture

ReelFS consists of three components:

- **Master Tracker** - Coordinates the cluster, maintains file location metadata, handles upload/download requests, and triggers replication
- **DataKeeper** - Stores files on disk, serves uploads/downloads over TCP, and communicates with the master via gRPC
- **Client CLI** - Command-line tool for uploading and downloading files

```
┌─────────────────────────────────────────────────────────────────────┐
│                            Network                                  │
│                                                                     │
│   ┌─────────┐         gRPC (internal)        ┌──────────────┐       │
│   │ Master  │◄──────────────────────────────►│  DataKeeper  │       │
│   │ Tracker │         Heartbeat/Replicate    │     (N)      │       │
│   └────┬────┘                                └──────┬───────┘       │
│        │                                            │               │
│        │ gRPC                                       │ TCP           │
│        │ (RequestUpload/Download)                   │ (file data)   │
│        │                                            │               │
│        └────────────────┬───────────────────────────┘               │
│                         │                                           │
│                    ┌────▼────┐                                      │
│                    │ Client  │                                      │
│                    │   CLI   │                                      │
│                    └─────────┘                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Communication

- **Client <-> Master**: gRPC for control plane (request upload/download, confirm upload)
- **Client <-> Keeper**: TCP for data plane (file transfers)
- **Keeper <-> Master**: gRPC for internal coordination (heartbeat, upload notifications, replication notifications)
- **Keeper <-> Keeper**: gRPC to initiate replication, TCP to transfer file data

### Address Types

Each DataKeeper has two addresses:

| Address Type | Protocol | Purpose | Visibility |
|--------------|----------|---------|------------|
| **Hostname** (gRPC) | gRPC | Internal communication between keepers and master | Private/Internal |
| **Address** (TCP) | TCP | File transfers with clients | Public |

## Prerequisites

- Go 1.24+
- [buf](https://buf.build/) - Protocol buffer tooling
- `protoc-gen-go` - Go protobuf code generator
- `protoc-gen-go-grpc` - Go gRPC code generator

**For Nix users**: All dependencies are included in `flake.nix`. Simply run:

```bash
nix develop
```

### Installing protoc plugins manually

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Ensure `$GOPATH/bin` is in your `PATH`.

## Building

### Generate protobuf code

```bash
buf generate
```

### Build all binaries

```bash
go build -o master ./cmd/master
go build -o keeper ./cmd/datakeeper
go build -o reelfs ./cmd/client
```

## Configuration

### Master Tracker

| Flag | Default | Description |
|------|---------|-------------|
| `-grpc-port` | `50040` | Port for the gRPC server (both client-facing and internal services) |
| `-replicas` | `3` | Target replication factor - how many copies of each file should exist across keepers |

**Hardcoded intervals:**
- Dead keeper detection: 5 seconds
- Replication check: 5 seconds
- Keeper marked dead after: 3 seconds without heartbeat

### DataKeeper

| Flag | Default | Description |
|------|---------|-------------|
| `-keeper-id` | `1` | Unique identifier for this keeper instance |
| `-data-dir` | `dataDir` | Directory path for storing files (created if not exists) |
| `-tcp-addr` | `50050` | Port for TCP file transfer server (public, for clients) |
| `-grpc-addr` | `50051` | Port for gRPC server (internal, for master/keeper communication) |
| `-master-addr` | `localhost:50040` | Address of the master tracker's gRPC server |

**Hardcoded behavior:**
- Heartbeat interval: 1 second
- Shutdown after 5 consecutive failed heartbeats

**Note:** Currently, the keeper registers with the master using `localhost:` prefix for both addresses. For production deployments across multiple machines, this would need to be modified to use actual hostnames/IPs.

### Client CLI

| Flag | Default | Description |
|------|---------|-------------|
| `--master-addr` | `localhost:50040` | Address of the master tracker's gRPC server |

## Quick Start

### Running a local cluster

**Terminal 1 - Start the Master:**

```bash
./master -grpc-port 50040 -replicas 2
```

**Terminal 2 - Start Keeper 1:**

```bash
./keeper -keeper-id k1 -data-dir ./data1 -tcp-addr 50050 -grpc-addr 50051 -master-addr localhost:50040
```

**Terminal 3 - Start Keeper 2:**

```bash
./keeper -keeper-id k2 -data-dir ./data2 -tcp-addr 50060 -grpc-addr 50061 -master-addr localhost:50040
```

**Terminal 4 - Start Keeper 3:**

```bash
./keeper -keeper-id k3 -data-dir ./data3 -tcp-addr 50070 -grpc-addr 50071 -master-addr localhost:50040
```

### Using the client

**Upload a file:**

```bash
./reelfs upload /path/to/video.mp4
```

**Download a file:**

```bash
./reelfs download video.mp4 ./downloads/
```

**Trigger master to log lookup tables (for debugging):**

```bash
./reelfs log
```

Or using grpcurl:

```bash
grpcurl -plaintext \
  -import-path ./proto \
  -proto masterpb/master.proto \
  localhost:50040 reelfs.MasterService/LogLookup
```

## How It Works

### Upload Flow

1. Client requests upload from master (filename, filesize)
2. Master selects a keeper via round-robin and returns its TCP address + transfer ID
3. Client uploads file directly to keeper over TCP
4. Keeper notifies master of successful upload
5. Client polls master to confirm upload completion
6. Master triggers replication to meet replication factor

### Download Flow

1. Client requests download from master (filename)
2. Master returns filesize and list of keepers that have the file
3. Client pings each keeper to verify availability
4. Client splits file into chunks and downloads in parallel from multiple keepers
5. Client reassembles chunks into final file

### Replication

- Master checks replication status every 5 seconds
- Files below the replication factor are queued for replication
- Source keeper pushes data to destination keeper over TCP
- Both keepers notify master upon completion
- Master updates its lookup tables

### Fault Tolerance

- Keepers send heartbeats every 1 second
- Master marks keepers as dead after 3 seconds without heartbeat
- Dead keeper removal runs every 5 seconds
- Files on dead keepers are re-replicated to healthy keepers
- Download retries failed chunks with different keepers (up to 5 attempts)

## File Storage

Files are stored in a hashed directory structure to avoid filesystem limitations with large numbers of files:

```
data-dir/
  ab/          # First 2 bytes of SHA256(filename)
    video1.mp4
  cd/
    video2.mp4
```

## Project Structure

```
reelfs/
  cmd/
    master/main.go      # Master tracker entry point
    datakeeper/main.go  # DataKeeper entry point
    client/main.go      # CLI client entry point
  
  internal/
    master/
      lookup.go         # Lookup tables and replication logic
      server.go         # gRPC service implementations
    keeper/
      storage.go        # File storage with hashed directories
      server.go         # gRPC service for replication commands
      tcp_handler.go    # TCP server for file transfers
    client/
      tcp_client.go     # TCP client for keeper communication
  
  pkg/
    protocol/           # TCP wire protocol (opcodes, metadata encoding)
  
  proto/
    shared/shared.proto       # Common message types
    masterpb/master.proto     # Master services and messages
    keeperpb/keeper.proto     # Keeper services and messages
  
  gen/                  # Generated protobuf code (do not edit)
```

