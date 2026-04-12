package rbdeal

import (
	"context"
	"testing"

	"github.com/CIDgravity/filecoin-gateway/ributil"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/stretchr/testify/require"
)

func TestOpenExistingWalletRejectsEmptyWallet(t *testing.T) {
	_, _, err := OpenExistingWallet(t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "no wallet found")
}

func TestOpenExistingWalletSetsDefaultForSingleKey(t *testing.T) {
	dir := t.TempDir()
	ks, err := ributil.OpenOrInitKeystore(dir)
	require.NoError(t, err)

	k, err := key.GenerateKey(types.KTSecp256k1)
	require.NoError(t, err)
	require.NoError(t, ks.Put(ributil.KNamePrefix+k.Address.String(), k.KeyInfo))

	wallet, addr, err := OpenExistingWallet(dir)
	require.NoError(t, err)
	require.Equal(t, k.Address, addr)

	def, err := wallet.GetDefault()
	require.NoError(t, err)
	require.Equal(t, k.Address, def)

	list, err := wallet.WalletList(context.Background())
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, k.Address, list[0])
}

func TestOpenOrCreateWalletStillCreatesWallet(t *testing.T) {
	_, addr, err := OpenOrCreateWallet(t.TempDir())
	require.NoError(t, err)
	require.NotEmpty(t, addr.String())
}
