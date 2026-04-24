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
