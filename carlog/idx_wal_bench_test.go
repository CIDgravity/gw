package carlog

import (
	"crypto/rand"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/multiformats/go-multihash"
)

// benchGenMhs generates N deterministic-ish multihashes for benchmarking.
func benchGenMhs(b *testing.B, n int) ([]multihash.Multihash, []int64) {
	b.Helper()
	mhs := make([]multihash.Multihash, n)
	offs := make([]int64, n)
	buf := make([]byte, 32)
	for i := range mhs {
		_, _ = rand.Read(buf)
		mh, _ := multihash.Sum(buf, multihash.SHA2_256, -1)
		mhs[i] = mh
		offs[i] = makeOffsetLen(int64(i*256), 200)
	}
	return mhs, offs
}

// -------- WAL Index Benchmarks --------

func BenchmarkWalIndex_Put(b *testing.B) {
	for _, n := range []int{100, 1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			mhs, offs := benchGenMhs(b, n)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				p := filepath.Join(b.TempDir(), "idx.wal")
				idx, err := CreateWalIndex(p)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if err := idx.Put(mhs, offs); err != nil {
					b.Fatal(err)
				}
				if err := idx.Sync(); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				_ = idx.Close()
			}
		})
	}
}

func BenchmarkWalIndex_Has(b *testing.B) {
	const N = 100_000
	mhs, offs := benchGenMhs(b, N)

	p := filepath.Join(b.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	if err != nil {
		b.Fatal(err)
	}
	if err := idx.Put(mhs, offs); err != nil {
		b.Fatal(err)
	}

	// Lookup subset
	query := mhs[:1000]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := idx.Has(query)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = idx.Close()
}

func BenchmarkWalIndex_Get(b *testing.B) {
	const N = 100_000
	mhs, offs := benchGenMhs(b, N)

	p := filepath.Join(b.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	if err != nil {
		b.Fatal(err)
	}
	if err := idx.Put(mhs, offs); err != nil {
		b.Fatal(err)
	}

	query := mhs[:1000]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := idx.Get(query)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = idx.Close()
}

func BenchmarkWalIndex_Replay(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			mhs, offs := benchGenMhs(b, n)

			p := filepath.Join(b.TempDir(), "idx.wal")
			idx, err := CreateWalIndex(p)
			if err != nil {
				b.Fatal(err)
			}
			if err := idx.Put(mhs, offs); err != nil {
				b.Fatal(err)
			}
			if err := idx.Sync(); err != nil {
				b.Fatal(err)
			}
			if err := idx.Close(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx, err := OpenWalIndex(p, -1)
				if err != nil {
					b.Fatal(err)
				}
				_ = idx.Close()
			}
		})
	}
}

func BenchmarkWalIndex_List(b *testing.B) {
	const N = 100_000
	mhs, offs := benchGenMhs(b, N)

	p := filepath.Join(b.TempDir(), "idx.wal")
	idx, err := CreateWalIndex(p)
	if err != nil {
		b.Fatal(err)
	}
	if err := idx.Put(mhs, offs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.List(func(c multihash.Multihash, o []int64) error {
			return nil
		})
	}
	b.StopTimer()
	_ = idx.Close()
}

// -------- LevelDB Index Benchmarks (comparison) --------

func BenchmarkLevelDB_Put(b *testing.B) {
	for _, n := range []int{100, 1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			mhs, offs := benchGenMhs(b, n)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				p := filepath.Join(b.TempDir(), "idx.level")
				idx, err := OpenLevelDBIndex(p, true)
				if err != nil {
					b.Fatal(err)
				}
				b.StartTimer()

				if err := idx.Put(mhs, offs); err != nil {
					b.Fatal(err)
				}
				if err := idx.Sync(); err != nil {
					b.Fatal(err)
				}

				b.StopTimer()
				_ = idx.Close()
			}
		})
	}
}

func BenchmarkLevelDB_Has(b *testing.B) {
	const N = 100_000
	mhs, offs := benchGenMhs(b, N)

	p := filepath.Join(b.TempDir(), "idx.level")
	idx, err := OpenLevelDBIndex(p, true)
	if err != nil {
		b.Fatal(err)
	}
	if err := idx.Put(mhs, offs); err != nil {
		b.Fatal(err)
	}

	query := mhs[:1000]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := idx.Has(query)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = idx.Close()
}

func BenchmarkLevelDB_Get(b *testing.B) {
	const N = 100_000
	mhs, offs := benchGenMhs(b, N)

	p := filepath.Join(b.TempDir(), "idx.level")
	idx, err := OpenLevelDBIndex(p, true)
	if err != nil {
		b.Fatal(err)
	}
	if err := idx.Put(mhs, offs); err != nil {
		b.Fatal(err)
	}

	query := mhs[:1000]

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := idx.Get(query)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = idx.Close()
}

func BenchmarkLevelDB_Replay(b *testing.B) {
	for _, n := range []int{1000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			mhs, offs := benchGenMhs(b, n)

			p := filepath.Join(b.TempDir(), "idx.level")
			idx, err := OpenLevelDBIndex(p, true)
			if err != nil {
				b.Fatal(err)
			}
			if err := idx.Put(mhs, offs); err != nil {
				b.Fatal(err)
			}
			if err := idx.Close(); err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx, err := OpenLevelDBIndex(p, false)
				if err != nil {
					b.Fatal(err)
				}
				_ = idx.Close()
			}
		})
	}
}
