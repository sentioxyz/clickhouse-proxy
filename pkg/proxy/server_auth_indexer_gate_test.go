package proxy

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestIsIndexerSigner(t *testing.T) {
	privKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	tests := []struct {
		name string
		ns   NetworkState
		addr string
		want bool
	}{
		{
			name: "nil NetworkState returns false",
			ns:   nil,
			addr: addr,
			want: false,
		},
		{
			name: "empty indexer list returns false",
			ns:   NewInMemoryNetworkState(),
			addr: addr,
			want: false,
		},
		{
			name: "matching signer returns true",
			ns: func() NetworkState {
				ns := NewInMemoryNetworkState()
				ns.IndexerInfos[1] = IndexerInfo{IndexerId: 1, Signer: addr}
				return ns
			}(),
			addr: addr,
			want: true,
		},
		{
			name: "case-insensitive match",
			ns: func() NetworkState {
				ns := NewInMemoryNetworkState()
				ns.IndexerInfos[1] = IndexerInfo{IndexerId: 1, Signer: strings.ToLower(addr)}
				return ns
			}(),
			addr: strings.ToUpper(addr),
			want: true,
		},
		{
			name: "non-matching signer returns false",
			ns: func() NetworkState {
				otherKey, _ := crypto.GenerateKey()
				otherAddr := crypto.PubkeyToAddress(otherKey.PublicKey).Hex()
				ns := NewInMemoryNetworkState()
				ns.IndexerInfos[1] = IndexerInfo{IndexerId: 1, Signer: otherAddr}
				return ns
			}(),
			addr: addr,
			want: false,
		},
		{
			name: "empty signer field is skipped",
			ns: func() NetworkState {
				ns := NewInMemoryNetworkState()
				ns.IndexerInfos[1] = IndexerInfo{IndexerId: 1, Signer: ""}
				return ns
			}(),
			addr: addr,
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isIndexerSigner(tt.ns, tt.addr)
			if got != tt.want {
				t.Errorf("isIndexerSigner() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsIndexerSigner_MultipleIndexers(t *testing.T) {
	key1, _ := crypto.GenerateKey()
	key2, _ := crypto.GenerateKey()
	addr1 := crypto.PubkeyToAddress(key1.PublicKey).Hex()
	addr2 := crypto.PubkeyToAddress(key2.PublicKey).Hex()

	ns := NewInMemoryNetworkState()
	ns.IndexerInfos[1] = IndexerInfo{IndexerId: 1, Signer: addr1}
	ns.IndexerInfos[2] = IndexerInfo{IndexerId: 2, Signer: addr2}

	if !isIndexerSigner(ns, addr1) {
		t.Error("addr1 should be recognized as indexer signer")
	}
	if !isIndexerSigner(ns, addr2) {
		t.Error("addr2 should be recognized as indexer signer")
	}

	key3, _ := crypto.GenerateKey()
	addr3 := crypto.PubkeyToAddress(key3.PublicKey).Hex()
	if isIndexerSigner(ns, addr3) {
		t.Error("addr3 should NOT be recognized as indexer signer")
	}
}
