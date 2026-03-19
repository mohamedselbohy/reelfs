grpcurl -plaintext \
  -import-path ./proto \
  -proto masterpb/master.proto \
  localhost:50040 reelfs.MasterService/LogLookup
