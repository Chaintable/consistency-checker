package check

import (
	"testing"

	"github.com/Chaintable/consistency-checker/config"
	"github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/common"
)

func TestBlockValidationPathLegacy(t *testing.T) {
	checker := &Checker{
		config: &config.Config{ChainID: 1},
	}
	hash := common.HexToHash("0x1234")
	block := &types.BlockContext{
		BlockNumber: 99,
		Hash:        hash,
	}

	if got, want := checker.blockValidationPrefix(block.BlockNumber), "1/99/"; got != want {
		t.Fatalf("blockValidationPrefix() = %q, want %q", got, want)
	}
	if got, want := checker.blockValidationKey(block), "1/99/"+hash.String(); got != want {
		t.Fatalf("blockValidationKey() = %q, want %q", got, want)
	}
	gotHash, ok := checker.blockValidationHashFromKey("1/99/" + hash.String())
	if !ok {
		t.Fatal("blockValidationHashFromKey() rejected legacy key")
	}
	if gotHash != hash.String() {
		t.Fatalf("blockValidationHashFromKey() = %q, want %q", gotHash, hash.String())
	}
}

func TestBlockValidationPathVersionMode(t *testing.T) {
	checker := &Checker{
		config: &config.Config{
			ChainID:                   1,
			Version:                   "v1",
			OuterVersionNewBlockTopic: "pipeline_1_v1",
		},
	}
	hash := common.HexToHash("0xabcd")
	block := &types.BlockContext{
		BlockNumber: 100,
		Hash:        hash,
	}

	if got, want := checker.blockValidationPrefix(block.BlockNumber), "1/v1/100/"; got != want {
		t.Fatalf("blockValidationPrefix() = %q, want %q", got, want)
	}
	if got, want := checker.blockValidationKey(block), "1/v1/100/"+hash.String(); got != want {
		t.Fatalf("blockValidationKey() = %q, want %q", got, want)
	}
	gotHash, ok := checker.blockValidationHashFromKey("1/v1/100/" + hash.String())
	if !ok {
		t.Fatal("blockValidationHashFromKey() rejected version key")
	}
	if gotHash != hash.String() {
		t.Fatalf("blockValidationHashFromKey() = %q, want %q", gotHash, hash.String())
	}
}

func TestBlockValidationHashFromKeyRejectsUnexpectedFormat(t *testing.T) {
	legacyChecker := &Checker{
		config: &config.Config{ChainID: 1},
	}
	if _, ok := legacyChecker.blockValidationHashFromKey("1/v1/99/0x1234"); ok {
		t.Fatal("blockValidationHashFromKey() accepted version key in legacy mode")
	}

	versionChecker := &Checker{
		config: &config.Config{
			ChainID:                   1,
			Version:                   "v1",
			OuterVersionNewBlockTopic: "pipeline_1_v1",
		},
	}
	if _, ok := versionChecker.blockValidationHashFromKey("1/99/0x1234"); ok {
		t.Fatal("blockValidationHashFromKey() accepted legacy key in version mode")
	}
}

// TestReorgReplayShortCircuit 复现生产 movr inner offset 443711 卡死场景：
// 一条 reorg 消息第一次已处理（latest 推进到 new 链尾 0x67dd0579），重试同一条时
// msgCheck 因 drop(0x42cd3d89) != latest(0x67dd0579) 返回 false（死锁根源），
// isAlreadyProcessed 应识别为已处理并短路。
func TestReorgReplayShortCircuit(t *testing.T) {
	dropHash := common.HexToHash("0x42cd3d890e490ee7641b5fa98d1b75ca3faa2b67ad7ce3f6802622e635ccc920")
	newMid := common.HexToHash("0x48d227ab3ddee35d9896c87926e6edd35a4180bb9833b5641053ad7612f7337f")
	newTail := common.HexToHash("0x67dd0579c899eee9561693d4f70a69dcfd754c934f4aad6c05f7074c99af089a")

	checker := &Checker{
		config: &config.Config{ChainID: 1285},
		latestOuterBlockChangeNotification: &types.OuterBlockChangeNotification{
			BlockNumber: 16881947,
			Hash:        newTail,
		},
	}
	reorg := &types.BlockChangeNotification{
		ChangeType: 2,
		DropBlocks: []types.BlockContext{{BlockNumber: 16881946, Hash: dropHash}},
		NewBlocks: []types.BlockContext{
			{BlockNumber: 16881946, Hash: newMid},
			{BlockNumber: 16881947, Hash: newTail},
		},
	}

	if checker.msgCheck(reorg) {
		t.Fatal("precondition: msgCheck should return false on reorg replay (drop != latest)")
	}
	if !checker.isAlreadyProcessed(reorg) {
		t.Fatal("isAlreadyProcessed should detect replayed reorg (newBlocks tail == latest) and short-circuit")
	}
}

func TestIsAlreadyProcessed(t *testing.T) {
	latest := common.HexToHash("0xaaaa")
	other := common.HexToHash("0xbbbb")

	c := &Checker{config: &config.Config{ChainID: 1}}
	if c.isAlreadyProcessed(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{{Hash: latest}}}) {
		t.Fatal("nil latest should return false")
	}

	c.latestOuterBlockChangeNotification = &types.OuterBlockChangeNotification{Hash: latest}
	if !c.isAlreadyProcessed(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{{Hash: other}, {Hash: latest}}}) {
		t.Fatal("newBlocks tail == latest should return true")
	}
	if c.isAlreadyProcessed(&types.BlockChangeNotification{NewBlocks: []types.BlockContext{{Hash: other}}}) {
		t.Fatal("newBlocks tail != latest should return false")
	}
	if c.isAlreadyProcessed(&types.BlockChangeNotification{NewBlocks: nil}) {
		t.Fatal("empty newBlocks should return false")
	}
}
