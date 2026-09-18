package check

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Chaintable/consistency-checker/db"
	"github.com/Chaintable/pipeline/types"
	"github.com/Chaintable/pipeline/util"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/segmentio/kafka-go"
)

func recoveryBlock(height uint64) types.BlockContext {
	return types.BlockContext{BlockNumber: height, Hash: common.BigToHash(new(big.Int).SetUint64(height)),
		ParentHash: common.BigToHash(new(big.Int).SetUint64(height - 1)), Timestamp: 1000 + height}
}

func recoveryTip(height uint64) *types.OuterBlockChangeNotification {
	b := recoveryBlock(height)
	return &types.OuterBlockChangeNotification{BlockNumber: height, Hash: b.Hash, Timestamp: b.Timestamp}
}

func recoveryNotice(height uint64) *types.BlockChangeNotification {
	return &types.BlockChangeNotification{ChangeType: 1, NewBlocks: []types.BlockContext{recoveryBlock(height)}}
}

func TestPlanStartupGap(t *testing.T) {
	tests := []struct {
		name    string
		tip     uint64
		target  uint64
		limit   uint64
		mutate  func(*types.BlockChangeNotification)
		readErr bool
		corrupt func(*types.BlockContext)
		want    []uint64
		wantErr bool
	}{
		{name: "single missing block", tip: 45, target: 47, limit: 4, want: []uint64{46}},
		{name: "multiple missing blocks", tip: 45, target: 49, limit: 3, want: []uint64{46, 47, 48}},
		{name: "resume published prefix", tip: 47, target: 49, limit: 3, want: []uint64{48}},
		{name: "contiguous", tip: 45, target: 46, limit: 4},
		{name: "duplicate", tip: 45, target: 45, limit: 4},
		{name: "disabled", tip: 45, target: 47},
		{name: "limit exceeded", tip: 45, target: 49, limit: 2, wantErr: true},
		{name: "explicit fork", tip: 45, target: 47, limit: 4, mutate: func(n *types.BlockChangeNotification) { n.ChangeType = 2 }},
		{name: "drop notification", tip: 45, target: 47, limit: 4, mutate: func(n *types.BlockChangeNotification) { n.DropBlocks = []types.BlockContext{recoveryBlock(45)} }},
		{name: "missing S3 object", tip: 45, target: 47, limit: 4, readErr: true, wantErr: true},
		{name: "wrong height", tip: 45, target: 47, limit: 4, corrupt: func(b *types.BlockContext) { b.BlockNumber++ }, wantErr: true},
		{name: "wrong hash", tip: 45, target: 47, limit: 4, corrupt: func(b *types.BlockContext) { b.Hash = common.Hash{} }, wantErr: true},
		{name: "fork does not reach tip", tip: 45, target: 47, limit: 4, corrupt: func(b *types.BlockContext) { b.ParentHash = common.Hash{} }, wantErr: true},
		{name: "invalid time", tip: 45, target: 47, limit: 4, corrupt: func(b *types.BlockContext) { b.Timestamp = 99999 }, wantErr: true},
		{name: "malformed original batch", tip: 45, target: 47, limit: 4, mutate: func(n *types.BlockChangeNotification) { n.NewBlocks = append(n.NewBlocks, recoveryBlock(49)) }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notice := recoveryNotice(tt.target)
			if tt.mutate != nil {
				tt.mutate(notice)
			}
			read := func(_ context.Context, hash common.Hash) (*types.BlockContext, error) {
				if tt.readErr {
					return nil, errors.New("S3 object missing")
				}
				b := recoveryBlock(hash.Big().Uint64())
				if tt.corrupt != nil {
					tt.corrupt(&b)
				}
				return &b, nil
			}
			blocks, err := planStartupGap(context.Background(), recoveryTip(tt.tip), notice, tt.limit, read)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, want error %v", err, tt.wantErr)
			}
			var heights []uint64
			for _, b := range blocks {
				heights = append(heights, b.BlockNumber)
			}
			if !reflect.DeepEqual(heights, tt.want) {
				t.Fatalf("heights = %v, want %v", heights, tt.want)
			}
		})
	}
}

func TestPlanStartupGapCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := planStartupGap(ctx, recoveryTip(45), recoveryNotice(47), 4, func(context.Context, common.Hash) (*types.BlockContext, error) {
		t.Fatal("read after cancellation")
		return nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func recoveryFile(height uint64) *types.BlockFile {
	b := recoveryBlock(height)
	return &types.BlockFile{Block: types.Block{ID: b.Hash.Hex(), Height: new(big.Int).SetUint64(height), ParentID: b.ParentHash.Hex(), Timestamp: b.Timestamp}}
}

// This fixture serves the actual gzip+JSON S3 format and runs the real Process
// path, including Pebble, validation reads and both outer destinations.
func newRecoveryChecker(t *testing.T, mutate func(map[string][]byte)) (*Checker, *fakeOuter) {
	t.Helper()
	c, outer := newVersionModeChecker(true)
	c.config.StartupGapRecoveryMaxBlocks = 4
	c.config.StartupGapRecoveryTimeoutMS = 1000
	c.config.OuterS3Bucket = "test-bucket"
	c.latestOuterBlockChangeNotification = recoveryTip(45)
	c.latestMsgOffset = 123         // Must never be modified by recovery/processing.
	c.lastWrittenBlockNumber = 1000 // No etcd update needed for unchanged ready replicas.
	c.quit = make(chan struct{})
	c.prefetchSem = make(chan struct{}, prefetchConcurrency)
	c.checkReplicas = func(uint64) (*ReplicaStateChangeNotification, error) {
		n := hexutil.Big(*big.NewInt(1000))
		return &ReplicaStateChangeNotification{LatestBlockNumber: &n}, nil
	}
	objects := map[string][]byte{}
	for h := uint64(45); h <= 52; h++ {
		file := recoveryFile(h)
		encoded, err := util.EncodeToJsonGzip(file)
		if err != nil {
			t.Fatal(err)
		}
		objects[fmt.Sprintf("1/v1/%s", file.Block.ID)] = encoded
		validation := file.Validation()
		encoded, err = util.EncodeToJsonGzip(validation)
		if err != nil {
			t.Fatal(err)
		}
		b := recoveryBlock(h)
		objects[c.blockValidationKey(&b)] = encoded
	}
	if mutate != nil {
		mutate(objects)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected S3 mutation: %s %s", r.Method, r.URL)
			w.WriteHeader(500)
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			type entry struct {
				Key string `xml:"Key"`
			}
			result := struct {
				XMLName     xml.Name `xml:"ListBucketResult"`
				Contents    []entry  `xml:"Contents"`
				IsTruncated bool     `xml:"IsTruncated"`
			}{}
			for key := range objects {
				if strings.HasPrefix(key, r.URL.Query().Get("prefix")) {
					result.Contents = append(result.Contents, entry{key})
				}
			}
			w.Header().Set("Content-Type", "application/xml")
			_ = xml.NewEncoder(w).Encode(result)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
		data, ok := objects[key]
		if !ok {
			w.WriteHeader(404)
			_, _ = w.Write([]byte("<Error><Code>NoSuchKey</Code></Error>"))
			return
		}
		_, _ = w.Write(data)
	}))
	t.Cleanup(server.Close)
	c.outerS3Reader = s3.NewFromConfig(aws.Config{Region: "us-east-1", Credentials: aws.AnonymousCredentials{}, HTTPClient: server.Client()}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
	previousDB := db.DB
	db.DB = nil
	if err := db.OpenConsistencyDB(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.DB.Close(); db.DB = previousDB })
	if err := db.DB.WriteBlockInfos([]types.BlockContext{recoveryBlock(45)}, []int64{0}); err != nil {
		t.Fatal(err)
	}
	return c, outer
}

func assertRecoveryWrites(t *testing.T, outer *fakeOuter, heights ...uint64) {
	t.Helper()
	var want []common.Hash
	for _, h := range heights {
		want = append(want, recoveryBlock(h).Hash)
	}
	for _, destination := range []string{outerVersionDestination, outerSingletonDestination} {
		if !reflect.DeepEqual(outer.writes[destination], want) {
			t.Fatalf("%s writes = %v, want %v", destination, outer.writes[destination], want)
		}
	}
}

func TestStartupRecoveryProcessesGapThenOriginalNoticeOnlyOnce(t *testing.T) {
	c, outer := newRecoveryChecker(t, nil)
	if !c.processInnerNotice(recoveryNotice(49), nil) {
		t.Fatal("recovery failed")
	}
	assertRecoveryWrites(t, outer, 46, 47, 48, 49)
	if c.latestMsgOffset != 123 {
		t.Fatal("recovery changed Kafka offset")
	}
	if !c.processInnerNotice(recoveryNotice(49), nil) {
		t.Fatal("duplicate retry failed")
	}
	assertRecoveryWrites(t, outer, 46, 47, 48, 49)
	if c.processInnerNotice(recoveryNotice(51), nil) {
		t.Fatal("runtime gap must not be recovered")
	}
	assertRecoveryWrites(t, outer, 46, 47, 48, 49)
}

func TestStartupRecoveryDisabled(t *testing.T) {
	c, outer := newRecoveryChecker(t, nil)
	c.config.StartupGapRecoveryMaxBlocks = 0
	if c.processInnerNotice(recoveryNotice(47), nil) {
		t.Fatal("disabled recovery accepted gap")
	}
	assertRecoveryWrites(t, outer)
}

func TestStartupRecoveryPartialPublishRetry(t *testing.T) {
	for _, failedDestination := range []string{outerVersionDestination, outerSingletonDestination} {
		t.Run(failedDestination, func(t *testing.T) {
			c, outer := newRecoveryChecker(t, nil)
			original := c.writeOuter
			fail := true
			c.writeOuter = func(w *kafka.Writer, b *types.OuterBlockChangeNotification) error {
				failedWriter := outer.version
				if failedDestination == outerSingletonDestination {
					failedWriter = outer.singleton
				}
				if fail && b.BlockNumber == 47 && w == failedWriter {
					return errors.New("injected publish failure")
				}
				return original(w, b)
			}
			if c.processInnerNotice(recoveryNotice(49), nil) {
				t.Fatal("expected partial publication failure")
			}
			if c.startupRecovered || c.latestOuterBlockChangeNotification.BlockNumber != 46 {
				t.Fatal("incorrect progress after failure")
			}
			fail = false
			if !c.processInnerNotice(recoveryNotice(49), nil) {
				t.Fatal("retry failed")
			}
			assertRecoveryWrites(t, outer, 46, 47, 48, 49)
			if c.latestMsgOffset != 123 {
				t.Fatal("changed original offset")
			}
		})
	}
}

func TestStartupRecoveryRestartResumesPublishedPrefix(t *testing.T) {
	c, outer := newRecoveryChecker(t, nil)
	original := c.writeOuter
	c.writeOuter = func(w *kafka.Writer, b *types.OuterBlockChangeNotification) error {
		if b.BlockNumber == 47 {
			return errors.New("both destinations unavailable")
		}
		return original(w, b)
	}
	if c.processInnerNotice(recoveryNotice(49), nil) {
		t.Fatal("expected failure after publishing 46")
	}
	// Reopen the durable database, and restore the tip read from the version
	// topic as NewChecker does. No in-memory delivery records survive restart.
	dir := db.DB.DBDir
	_ = db.DB.Close()
	db.DB = nil
	if err := db.OpenConsistencyDB(dir); err != nil {
		t.Fatal(err)
	}
	c.latestOuterBlockChangeNotification = recoveryTip(46)
	c.currentNotice = noticeID{}
	c.delivered = nil
	c.startupRecovered = false
	c.writeOuter = original
	if !c.processInnerNotice(recoveryNotice(49), nil) {
		t.Fatal("restart recovery failed")
	}
	assertRecoveryWrites(t, outer, 46, 47, 48, 49)
}

func TestStartupRecoveryRejectsS3InconsistencyBeforeAnyPublish(t *testing.T) {
	for _, kind := range []string{"missing file", "missing validation", "validation mismatch", "marked fork", "wrong block id", "malformed file"} {
		t.Run(kind, func(t *testing.T) {
			c, outer := newRecoveryChecker(t, func(objects map[string][]byte) {
				b := recoveryBlock(47)
				fileKey := "1/v1/" + b.Hash.Hex()
				validationKey := fmt.Sprintf("1/v1/47/%s", b.Hash.Hex())
				switch kind {
				case "missing file":
					delete(objects, fileKey)
				case "missing validation":
					delete(objects, validationKey)
				case "malformed file":
					objects[fileKey] = []byte("not gzip")
				case "wrong block id":
					file := recoveryFile(47)
					file.Block.ID = recoveryBlock(48).Hash.Hex()
					objects[fileKey], _ = util.EncodeToJsonGzip(file)
				default:
					v := recoveryFile(47).Validation()
					if kind == "marked fork" {
						v.IsFork = true
					} else {
						v.TracesCount++
					}
					objects[validationKey], _ = util.EncodeToJsonGzip(v)
				}
			})
			if c.processInnerNotice(recoveryNotice(49), nil) {
				t.Fatal("accepted invalid recovery data")
			}
			assertRecoveryWrites(t, outer)
			if c.latestMsgOffset != 123 || c.latestOuterBlockChangeNotification.BlockNumber != 45 || c.startupRecovered {
				t.Fatal("advanced state after invalid plan")
			}
		})
	}
}

func TestStartupRecoveryHonorsShutdown(t *testing.T) {
	c, outer := newRecoveryChecker(t, nil)
	close(c.quit)
	if c.processInnerNotice(recoveryNotice(49), nil) {
		t.Fatal("recovered after shutdown")
	}
	assertRecoveryWrites(t, outer)
}

func TestReadRecoveryBlockTimeout(t *testing.T) {
	c, _ := newRecoveryChecker(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	if _, err := c.readRecoveryBlock(ctx, recoveryBlock(46).Hash); err == nil {
		t.Fatal("read ignored expired context")
	}
}
