package headtracker

import (
	"context"
	"sync"

	"github.com/smartcontractkit/chainlink/core/services/eth"
)

func GetHeadListenerConnectedMutex(hl *HeadListener) *sync.RWMutex {
	return &hl.connectedMutex
}

func AddHeads(hs *HeadSaver, heads []*eth.Head, historyDepth int) {
	hs.addHeads(heads, historyDepth)
}

func LoadFromDB(ht *HeadTracker) (*eth.Head, error) {
	return ht.headSaver.LoadFromDB(context.Background())
}
