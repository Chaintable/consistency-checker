package check

import (
	"os"
	"testing"

	"github.com/Chaintable/consistency-checker/config"
	"github.com/Chaintable/pipeline/util"
	"github.com/ethereum/go-ethereum/common"
)

// TestGetVersionBlockByHashIT 对真实 movr outer S3 验证 fix B：
// 之前 getVersionBlockByHash 读 {hash}/block（只在 inner bucket）对任意 hash 都 404，
// 导致 align fork 分支永久失败。改读 outer blockfile {hash} 后应能成功。
// 需要 AWS 凭证，仅在 CHECKER_S3_IT=1 时运行；CI 的 go test ./... 默认跳过。
func TestGetVersionBlockByHashIT(t *testing.T) {
	if os.Getenv("CHECKER_S3_IT") != "1" {
		t.Skip("set CHECKER_S3_IT=1 to run (needs AWS creds + prod outer S3 read)")
	}

	cfg := &config.Config{
		ChainID:       1285,
		Version:       "fc957cc2",
		OuterS3Bucket: "chaintable-pipeline--apne1-az4--x-s3",
		OuterS3Region: "ap-northeast-1",
	}
	s3r, err := util.NewS3Client(cfg.OuterS3Region)
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}
	c := &Checker{config: cfg, outerS3Reader: s3r}

	cases := []struct {
		name       string
		hash       string
		wantHeight uint64
		wantParent string
	}{
		{
			// 当初 align 404 的 fork 块（被 reorg 丢弃）
			name:       "forked-block-0x42cd3d89",
			hash:       "0x42cd3d890e490ee7641b5fa98d1b75ca3faa2b67ad7ce3f6802622e635ccc920",
			wantHeight: 16881946,
			wantParent: "0xdf6eec0a625f1b512cece2ca7f9dd08030d97f2e4f67e7aa3b5946a0b1c952ec",
		},
		{
			// 取代它的 canonical 块
			name:       "canonical-block-0x48d227ab",
			hash:       "0x48d227ab3ddee35d9896c87926e6edd35a4180bb9833b5641053ad7612f7337f",
			wantHeight: 16881946,
			wantParent: "0xdf6eec0a625f1b512cece2ca7f9dd08030d97f2e4f67e7aa3b5946a0b1c952ec",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bc, err := c.getVersionBlockByHash(common.HexToHash(tc.hash))
			if err != nil {
				t.Fatalf("getVersionBlockByHash(%s) error: %v", tc.hash, err)
			}
			if bc.BlockNumber != tc.wantHeight {
				t.Errorf("BlockNumber = %d, want %d", bc.BlockNumber, tc.wantHeight)
			}
			if bc.ParentHash != common.HexToHash(tc.wantParent) {
				t.Errorf("ParentHash = %s, want %s", bc.ParentHash.Hex(), tc.wantParent)
			}
			if bc.Hash != common.HexToHash(tc.hash) {
				t.Errorf("Hash = %s, want %s", bc.Hash.Hex(), tc.hash)
			}
			if bc.Timestamp == 0 {
				t.Errorf("Timestamp = 0, want non-zero")
			}
			t.Logf("OK %s: height=%d parent=%s ts=%d", tc.name, bc.BlockNumber, bc.ParentHash.Hex(), bc.Timestamp)
		})
	}
}
