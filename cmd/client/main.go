package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"reelfs/gen/masterpb"
	"reelfs/gen/shared"
	"reelfs/internal/client"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var masterAddr string

func main() {
	rootCmd := &cobra.Command{Use: "reelfs", Short: "ReelFS distributed file system client"}
	rootCmd.PersistentFlags().StringVar(&masterAddr, "master-addr", "localhost:50040", "Master gRPC Address")
	uploadCmd := &cobra.Command{
		Use:   "upload",
		Short: "Upload a file",
		Args:  cobra.ExactArgs(1),
		RunE:  runUpload,
	}
	logCmd := &cobra.Command{
		Use:   "log",
		Short: "Trigger log for master",
		RunE:  runLog,
	}
	downloadCmd := &cobra.Command{
		Use:   "download",
		Short: "Download a file",
		Args:  cobra.ExactArgs(2),
		RunE:  runDownload,
	}
	rootCmd.AddCommand(uploadCmd, downloadCmd, logCmd)
	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("%v", err)
		os.Exit(1)
	}
}

func runDownload(cmd *cobra.Command, args []string) error {
	filename := args[0]
	destDir := args[1]
	if filename == "" || destDir == "" {
		return errors.New("both filename and destination directory are required")
	}
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("connecting to master: %v", err)
		return nil
	}
	defer conn.Close()
	masterClient := masterpb.NewMasterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	res, err := masterClient.RequestDownload(ctx, &masterpb.DownloadRequest{
		Filename: filename,
	})
	cancel()
	if err != nil {
		fmt.Printf("requesting download: %v", err)
		return nil
	}
	if !res.Success {
		fmt.Printf("download request failed: %s", res.Msg)
		return nil
	}
	var keepers []*shared.KeeperInfo
	for _, keeper := range res.Keepers {
		tcpClient, err := client.NewTCPClient(keeper.Address)
		if err != nil {
			continue
		}
		err = tcpClient.CheckDownload(filename, uint64(res.Filesize))
		tcpClient.Close()
		if err != nil {
			continue
		}
		keepers = append(keepers, keeper)
	}
	if len(keepers) == 0 {
		fmt.Printf("no keepers available for this file %s", filename)
		return nil
	}
	tmpDir, err := os.MkdirTemp("", ".reelfs-download-*")
	if err != nil {
		fmt.Print("failed to create tmp directory to store chunks")
		return nil
	}
	defer os.RemoveAll(tmpDir)
	type ChunkTask struct {
		Index  int
		Offset int64
		Length int64
		Keeper *shared.KeeperInfo
	}
	chunkSize := res.Filesize / int64(len(keepers))
	rem := res.Filesize % int64(len(keepers))
	var tasks []ChunkTask
	for i, keeper := range keepers {
		length := chunkSize
		if i == len(keepers)-1 {
			length += rem
		}
		tasks = append(tasks, ChunkTask{
			Index:  i,
			Offset: int64(i) * chunkSize,
			Length: length,
			Keeper: keeper,
		})
	}
	totalChunks := len(tasks)
	aliveKeepers := make([]*shared.KeeperInfo, len(keepers))
	copy(aliveKeepers, keepers)
	p := mpb.New(mpb.WithWidth(60))
	successful := false
	for attempt := range 5 {
		if len(tasks) == 0 {
			successful = true
			break
		}
		if len(aliveKeepers) == 0 {
			break
		}
		for i := range tasks {
			tasks[i].Keeper = aliveKeepers[i%len(aliveKeepers)]
		}
		bars := make(map[int]*mpb.Bar)
		for _, task := range tasks {
			label := fmt.Sprintf("chunk %d [%s] (attempt %d)",
				task.Index, task.Keeper.Address, attempt+1)

			bar := p.AddBar(task.Length,
				mpb.PrependDecorators(
					decor.Name(label, decor.WCSyncSpaceR),
					decor.CountersKibiByte("% .2f / % .2f"),
				),
				mpb.AppendDecorators(
					decor.EwmaETA(decor.ET_STYLE_GO, 60),
					decor.Name(" | "),
					decor.EwmaSpeed(decor.SizeB1024(0), "% .2f", 60),
				),
			)
			bars[task.Index] = bar
		}
		ch := make(chan ChunkTask, len(tasks))
		var failed []ChunkTask
		failedKeepers := make(map[string]struct{})
		var mu sync.Mutex
		var wg sync.WaitGroup
		sem := make(chan struct{}, 10)
		wg.Add(len(tasks))
		for _, task := range tasks {
			sem <- struct{}{}
			go func(task ChunkTask) {
				defer wg.Done()
				defer func() { <-sem }()
				bar := bars[task.Index]
				markFailed := func(keeperFault bool) {
					bar.Abort(true)
					mu.Lock()
					if keeperFault {
						failedKeepers[task.Keeper.Address] = struct{}{}
					}
					failed = append(failed, task)
					mu.Unlock()
				}
				tcpClient, err := client.NewTCPClient(task.Keeper.Address)
				if err != nil {
					markFailed(true)
					return
				}
				defer tcpClient.Close()
				data, err := tcpClient.Download(filename, uint64(task.Offset), uint64(task.Length))
				if err != nil {
					markFailed(true)
					return
				}
				f, err := os.Create(filepath.Join(tmpDir, fmt.Sprintf("%d.chunk", task.Index)))
				if err != nil {
					markFailed(false)
					return
				}
				defer f.Close()
				reader := bar.ProxyReader(data)
				defer reader.Close()
				_, err = io.Copy(f, reader)
				if err != nil {
					markFailed(true)
					return
				}
				ch <- task
			}(task)
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		for range ch {
		}
		if len(failed) == 0 {
			successful = true
			break
		}
		var newAlive []*shared.KeeperInfo
		for _, k := range aliveKeepers {
			if _, bad := failedKeepers[k.Address]; !bad {
				newAlive = append(newAlive, k)
			}
		}
		aliveKeepers = newAlive
		tasks = failed
	}
	p.Wait()
	if !successful {
		fmt.Printf("could not download file %s keepers irresponsive", filename)
		return nil
	}
	finalPath := filepath.Join(destDir, filename)
	finalFile, err := os.Create(finalPath)
	if err != nil {
		fmt.Printf("creating final file: %v", err)
		return nil
	}
	defer finalFile.Close()
	for i := range totalChunks {
		chunkPath := filepath.Join(tmpDir, fmt.Sprintf("%d.chunk", i))
		chunk, err := os.Open(chunkPath)
		if err != nil {
			os.Remove(finalPath)
			fmt.Printf("opening chunk %d: %v", i, err)
			return nil
		}
		_, err = io.Copy(finalFile, chunk)
		chunk.Close()
		if err != nil {
			os.Remove(finalPath)
			fmt.Printf("writing chunk %d: %v", i, err)
			return nil
		}
	}
	fmt.Println("download successful")
	return nil
}

func runUpload(cmd *cobra.Command, args []string) error {
	filePath := args[0]
	filename := filepath.Base(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}
	filesize := stat.Size()
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to master: %w", err)
	}
	defer conn.Close()
	masterClient := masterpb.NewMasterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	res, err := masterClient.RequestUpload(ctx, &masterpb.UploadRequest{
		Filename: filename,
		Filesize: filesize,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("uploading to keeper: %w", err)
	}
	if !res.Success {
		return fmt.Errorf("upload request denied: %w", err)
	}
	fmt.Printf("Uploading to keeper: %s", res.KeeperAddress)

	tcpClient, err := client.NewTCPClient(res.KeeperAddress)
	if err != nil {
		return fmt.Errorf("connecting to keeper: %w", err)
	}
	defer tcpClient.Close()

	bar := progressbar.NewOptions64(
		filesize,
		progressbar.OptionSetDescription(fmt.Sprintf("[\033[32m%s\033[0m]", filename)),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(40),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(os.Stderr, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionSetRenderBlankState(true),
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
	)
	err = tcpClient.UploadWithProgress(res.TransferId, filename, file, uint64(filesize), bar)
	if err != nil {
		return fmt.Errorf("uploading file: %w", err)
	}
	fmt.Print("Confirming Upload...")
	for i := range 5 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		confirmRes, err := masterClient.ConfirmUpload(ctx, &masterpb.ConfirmUploadRequest{
			TransferId: res.TransferId,
		})
		cancel()
		if err != nil {
			fmt.Printf("\rConfirm error, retrying... (%d/5)\n", i+1)
			time.Sleep(time.Duration(1<<i) * time.Second)
			continue
		}
		switch confirmRes.Status {
		case masterpb.ConfirmUploadResponse_SUCCESS:
			fmt.Print("\r\033[32m✓ Upload complete!\033[0m\n")
			return nil
		case masterpb.ConfirmUploadResponse_FAILED:
			return fmt.Errorf("upload failed: %s", confirmRes.Msg)
		case masterpb.ConfirmUploadResponse_MISSING:
			return errors.New("upload not found on master")
		case masterpb.ConfirmUploadResponse_PENDING:
			fmt.Printf("\rWaiting for confirmation... (%d/5)", i+1)
			time.Sleep(time.Duration(1<<i) * time.Second)
		}
	}
	return errors.New("upload confirmation timed out")
}

func runLog(cmd *cobra.Command, args []string) error {
	conn, err := grpc.NewClient(masterAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connecting to master: %w", err)
	}
	defer conn.Close()

	masterClient := masterpb.NewMasterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = masterClient.LogLookup(ctx, &masterpb.LogRequest{})
	if err != nil {
		return fmt.Errorf("calling loglookup: %w", err)
	}
	return nil
}
