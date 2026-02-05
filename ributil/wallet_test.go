package ributil

import (
	"context"
	"errors"
	"testing"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
)

// DiskKeyStore Tests

func TestDiskKeyStore_PutGet(t *testing.T) {
	dir := t.TempDir()

	ks, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	testKey := types.KeyInfo{
		Type:       types.KTSecp256k1,
		PrivateKey: []byte("test-private-key-data"),
	}

	// Put a key
	err = ks.Put("test-key", testKey)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Get the key back
	got, err := ks.Get("test-key")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if got.Type != testKey.Type {
		t.Errorf("Get() Type = %v, want %v", got.Type, testKey.Type)
	}
	if string(got.PrivateKey) != string(testKey.PrivateKey) {
		t.Errorf("Get() PrivateKey = %v, want %v", got.PrivateKey, testKey.PrivateKey)
	}
}

func TestDiskKeyStore_PutDuplicate(t *testing.T) {
	dir := t.TempDir()

	ks, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	testKey := types.KeyInfo{
		Type:       types.KTSecp256k1,
		PrivateKey: []byte("test-private-key-data"),
	}

	// Put a key
	err = ks.Put("test-key", testKey)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Try to put the same key again - should error
	err = ks.Put("test-key", testKey)
	if err == nil {
		t.Error("Put() expected error for duplicate key, got nil")
	}
	if !errors.Is(err, types.ErrKeyExists) {
		t.Errorf("Put() error = %v, want error wrapping ErrKeyExists", err)
	}
}

func TestDiskKeyStore_List(t *testing.T) {
	dir := t.TempDir()

	ks, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	// Initially empty
	keys, err := ks.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("List() got %d keys, want 0", len(keys))
	}

	// Add some keys
	testKeys := []string{"key1", "key2", "key3"}
	for _, name := range testKeys {
		err = ks.Put(name, types.KeyInfo{
			Type:       types.KTSecp256k1,
			PrivateKey: []byte("private-" + name),
		})
		if err != nil {
			t.Fatalf("Put(%s) error = %v", name, err)
		}
	}

	// List should return all keys
	keys, err = ks.List()
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(keys) != len(testKeys) {
		t.Errorf("List() got %d keys, want %d", len(keys), len(testKeys))
	}

	// Verify all keys are present
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k] = true
	}
	for _, expected := range testKeys {
		if !keySet[expected] {
			t.Errorf("List() missing key %q", expected)
		}
	}
}

func TestDiskKeyStore_Delete(t *testing.T) {
	dir := t.TempDir()

	ks, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	testKey := types.KeyInfo{
		Type:       types.KTSecp256k1,
		PrivateKey: []byte("test-private-key-data"),
	}

	// Put a key
	err = ks.Put("test-key", testKey)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Verify it exists
	_, err = ks.Get("test-key")
	if err != nil {
		t.Fatalf("Get() before delete error = %v", err)
	}

	// Delete the key
	err = ks.Delete("test-key")
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	// Verify it no longer exists
	_, err = ks.Get("test-key")
	if err == nil {
		t.Error("Get() after delete expected error, got nil")
	}
	if !errors.Is(err, types.ErrKeyInfoNotFound) {
		t.Errorf("Get() after delete error = %v, want error wrapping ErrKeyInfoNotFound", err)
	}
}

func TestDiskKeyStore_DeleteNonExistent(t *testing.T) {
	dir := t.TempDir()

	ks, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	// Try to delete a key that doesn't exist
	err = ks.Delete("nonexistent-key")
	if err == nil {
		t.Error("Delete() expected error for non-existent key, got nil")
	}
	if !errors.Is(err, types.ErrKeyInfoNotFound) {
		t.Errorf("Delete() error = %v, want error wrapping ErrKeyInfoNotFound", err)
	}
}

func TestDiskKeyStore_Has(t *testing.T) {
	dir := t.TempDir()

	ks, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	// Key doesn't exist initially
	_, err = ks.Get("test-key")
	if err == nil {
		t.Error("Get() expected error for non-existent key, got nil")
	}
	if !errors.Is(err, types.ErrKeyInfoNotFound) {
		t.Errorf("Get() error = %v, want error wrapping ErrKeyInfoNotFound", err)
	}

	// Put a key
	err = ks.Put("test-key", types.KeyInfo{
		Type:       types.KTSecp256k1,
		PrivateKey: []byte("test-data"),
	})
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Key should now exist
	_, err = ks.Get("test-key")
	if err != nil {
		t.Errorf("Get() after Put error = %v, want nil", err)
	}
}

func TestDiskKeyStore_NotFound(t *testing.T) {
	dir := t.TempDir()

	ks, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	// Get a key that doesn't exist
	_, err = ks.Get("nonexistent-key")
	if err == nil {
		t.Error("Get() expected error for non-existent key, got nil")
	}
	if !errors.Is(err, types.ErrKeyInfoNotFound) {
		t.Errorf("Get() error = %v, want error wrapping ErrKeyInfoNotFound", err)
	}
}

// LocalWallet Tests

func TestOpenWallet_New(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}
	if wallet == nil {
		t.Error("OpenWallet() returned nil wallet")
	}
}

func TestOpenWallet_Existing(t *testing.T) {
	dir := t.TempDir()

	// Create wallet first time
	wallet1, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() first time error = %v", err)
	}

	// Generate a key
	ctx := context.Background()
	addr, err := wallet1.WalletNew(ctx, types.KTSecp256k1)
	if err != nil {
		t.Fatalf("WalletNew() error = %v", err)
	}

	// Open wallet again
	wallet2, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() second time error = %v", err)
	}

	// List addresses - should contain the address created earlier
	addrs, err := wallet2.WalletList(ctx)
	if err != nil {
		t.Fatalf("WalletList() error = %v", err)
	}

	found := false
	for _, a := range addrs {
		if a == addr {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("WalletList() did not contain address %s created in previous session", addr)
	}
}

func TestWalletNew(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	ctx := context.Background()
	addr, err := wallet.WalletNew(ctx, types.KTSecp256k1)
	if err != nil {
		t.Fatalf("WalletNew() error = %v", err)
	}

	// Address should not be undefined
	if addr == address.Undef {
		t.Error("WalletNew() returned undefined address")
	}

	// Address should be a secp256k1 address (protocol 1)
	if addr.Protocol() != address.SECP256K1 {
		t.Errorf("WalletNew() address protocol = %d, want %d (SECP256K1)", addr.Protocol(), address.SECP256K1)
	}
}

func TestWalletList(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	ctx := context.Background()

	// Initially empty
	addrs, err := wallet.WalletList(ctx)
	if err != nil {
		t.Fatalf("WalletList() error = %v", err)
	}
	if len(addrs) != 0 {
		t.Errorf("WalletList() got %d addresses, want 0", len(addrs))
	}

	// Create a few addresses
	createdAddrs := make([]address.Address, 3)
	for i := 0; i < 3; i++ {
		addr, err := wallet.WalletNew(ctx, types.KTSecp256k1)
		if err != nil {
			t.Fatalf("WalletNew() error = %v", err)
		}
		createdAddrs[i] = addr
	}

	// List should return all addresses
	addrs, err = wallet.WalletList(ctx)
	if err != nil {
		t.Fatalf("WalletList() error = %v", err)
	}
	if len(addrs) != 3 {
		t.Errorf("WalletList() got %d addresses, want 3", len(addrs))
	}

	// All created addresses should be in the list
	addrSet := make(map[address.Address]bool)
	for _, a := range addrs {
		addrSet[a] = true
	}
	for _, created := range createdAddrs {
		if !addrSet[created] {
			t.Errorf("WalletList() missing address %s", created)
		}
	}
}

func TestWalletSign(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	ctx := context.Background()
	addr, err := wallet.WalletNew(ctx, types.KTSecp256k1)
	if err != nil {
		t.Fatalf("WalletNew() error = %v", err)
	}

	msg := []byte("test message to sign")

	sig, err := wallet.WalletSign(ctx, addr, msg, api.MsgMeta{})
	if err != nil {
		t.Fatalf("WalletSign() error = %v", err)
	}

	if sig == nil {
		t.Error("WalletSign() returned nil signature")
	}

	// Signature should have some data
	if len(sig.Data) == 0 {
		t.Error("WalletSign() returned signature with no data")
	}
}

func TestWalletSign_NonExistentKey(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	ctx := context.Background()

	// Create an address that doesn't exist in the wallet
	// We'll use a known address format
	nonExistentAddr, err := address.NewFromString("f1abjxfbp274xpdqcpuaykwkfb43omjotacm2p3za")
	if err != nil {
		t.Fatalf("NewFromString() error = %v", err)
	}

	msg := []byte("test message")

	_, err = wallet.WalletSign(ctx, nonExistentAddr, msg, api.MsgMeta{})
	if err == nil {
		t.Error("WalletSign() expected error for non-existent key, got nil")
	}
}

func TestGetDefault(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	ctx := context.Background()

	// No default initially
	_, err = wallet.GetDefault()
	if err == nil {
		t.Error("GetDefault() expected error when no default set, got nil")
	}

	// Create a key - this should become the default
	addr, err := wallet.WalletNew(ctx, types.KTSecp256k1)
	if err != nil {
		t.Fatalf("WalletNew() error = %v", err)
	}

	// Now default should be set
	defaultAddr, err := wallet.GetDefault()
	if err != nil {
		t.Fatalf("GetDefault() error = %v", err)
	}
	if defaultAddr != addr {
		t.Errorf("GetDefault() = %s, want %s", defaultAddr, addr)
	}
}

func TestSetDefault(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	ctx := context.Background()

	// Create two keys
	addr1, err := wallet.WalletNew(ctx, types.KTSecp256k1)
	if err != nil {
		t.Fatalf("WalletNew() first error = %v", err)
	}

	addr2, err := wallet.WalletNew(ctx, types.KTSecp256k1)
	if err != nil {
		t.Fatalf("WalletNew() second error = %v", err)
	}

	// First key should be default
	defaultAddr, err := wallet.GetDefault()
	if err != nil {
		t.Fatalf("GetDefault() error = %v", err)
	}
	if defaultAddr != addr1 {
		t.Errorf("GetDefault() = %s, want %s (first created)", defaultAddr, addr1)
	}

	// Set second key as default
	err = wallet.SetDefault(addr2)
	if err != nil {
		t.Fatalf("SetDefault() error = %v", err)
	}

	// Second key should now be default
	defaultAddr, err = wallet.GetDefault()
	if err != nil {
		t.Fatalf("GetDefault() after SetDefault error = %v", err)
	}
	if defaultAddr != addr2 {
		t.Errorf("GetDefault() = %s, want %s (second created)", defaultAddr, addr2)
	}
}

func TestSetDefault_NonExistentKey(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	// Try to set a non-existent address as default
	nonExistentAddr, err := address.NewFromString("f1abjxfbp274xpdqcpuaykwkfb43omjotacm2p3za")
	if err != nil {
		t.Fatalf("NewFromString() error = %v", err)
	}

	err = wallet.SetDefault(nonExistentAddr)
	if err == nil {
		t.Error("SetDefault() expected error for non-existent key, got nil")
	}
}

func TestWalletExport_Import(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	// Create first wallet and generate a key
	wallet1, err := OpenWallet(dir1)
	if err != nil {
		t.Fatalf("OpenWallet() wallet1 error = %v", err)
	}

	ctx := context.Background()
	addr, err := wallet1.WalletNew(ctx, types.KTSecp256k1)
	if err != nil {
		t.Fatalf("WalletNew() error = %v", err)
	}

	// Export the key via the keystore
	// We need to access the keystore directly since there's no WalletExport method
	ks1, err := OpenOrInitKeystore(dir1)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}

	keyInfo, err := ks1.Get(KNamePrefix + addr.String())
	if err != nil {
		t.Fatalf("Get() keyInfo error = %v", err)
	}

	// Create second wallet and import the key
	ks2, err := OpenOrInitKeystore(dir2)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() wallet2 error = %v", err)
	}

	err = ks2.Put(KNamePrefix+addr.String(), keyInfo)
	if err != nil {
		t.Fatalf("Put() import error = %v", err)
	}

	// Open wallet2 and verify the key is there
	wallet2, err := OpenWallet(dir2)
	if err != nil {
		t.Fatalf("OpenWallet() wallet2 after import error = %v", err)
	}

	addrs, err := wallet2.WalletList(ctx)
	if err != nil {
		t.Fatalf("WalletList() error = %v", err)
	}

	found := false
	for _, a := range addrs {
		if a == addr {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Imported key %s not found in wallet2", addr)
	}

	// Verify we can sign with the imported key
	msg := []byte("test message after import")
	sig, err := wallet2.WalletSign(ctx, addr, msg, api.MsgMeta{})
	if err != nil {
		t.Fatalf("WalletSign() with imported key error = %v", err)
	}
	if sig == nil || len(sig.Data) == 0 {
		t.Error("WalletSign() with imported key returned empty signature")
	}
}

func TestOpenOrInitKeystore_NewDirectory(t *testing.T) {
	dir := t.TempDir()
	newPath := dir + "/newkeystore"

	ks, err := OpenOrInitKeystore(newPath)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() error = %v", err)
	}
	if ks == nil {
		t.Error("OpenOrInitKeystore() returned nil")
	}

	// Keystore should be usable
	err = ks.Put("test", types.KeyInfo{
		Type:       types.KTSecp256k1,
		PrivateKey: []byte("test"),
	})
	if err != nil {
		t.Errorf("Put() on new keystore error = %v", err)
	}
}

func TestOpenOrInitKeystore_ExistingDirectory(t *testing.T) {
	dir := t.TempDir()

	// First call creates it
	ks1, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() first time error = %v", err)
	}

	err = ks1.Put("existing-key", types.KeyInfo{
		Type:       types.KTSecp256k1,
		PrivateKey: []byte("existing-data"),
	})
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	// Second call opens existing
	ks2, err := OpenOrInitKeystore(dir)
	if err != nil {
		t.Fatalf("OpenOrInitKeystore() second time error = %v", err)
	}

	// Should be able to read the key created by ks1
	keyInfo, err := ks2.Get("existing-key")
	if err != nil {
		t.Fatalf("Get() existing key error = %v", err)
	}
	if string(keyInfo.PrivateKey) != "existing-data" {
		t.Errorf("Get() PrivateKey = %s, want %s", keyInfo.PrivateKey, "existing-data")
	}
}

// Test that addresses are sorted in WalletList
func TestWalletList_Sorted(t *testing.T) {
	dir := t.TempDir()

	wallet, err := OpenWallet(dir)
	if err != nil {
		t.Fatalf("OpenWallet() error = %v", err)
	}

	ctx := context.Background()

	// Create multiple addresses
	for i := 0; i < 5; i++ {
		_, err := wallet.WalletNew(ctx, types.KTSecp256k1)
		if err != nil {
			t.Fatalf("WalletNew() error = %v", err)
		}
	}

	addrs, err := wallet.WalletList(ctx)
	if err != nil {
		t.Fatalf("WalletList() error = %v", err)
	}

	// Verify addresses are sorted
	for i := 1; i < len(addrs); i++ {
		if addrs[i-1].String() > addrs[i].String() {
			t.Errorf("WalletList() addresses not sorted: %s > %s", addrs[i-1], addrs[i])
		}
	}
}
