package byterate

import (
	"testing"

	"github.com/djylb/byterate/internal/leakcheck"
)

func TestMain(m *testing.M) { leakcheck.VerifyTestMain(m) }
