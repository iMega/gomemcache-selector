package gomemcacheselector_test

import (
	"crypto/md5"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	go_ketama "github.com/dgryski/go-ketama"
	"github.com/imega/gomemcache-selector/dgryski"
	"github.com/imega/gomemcache-selector/ngerakines"
	goketama "github.com/rckclmbr/goketama/ketama"
)

func TestServerList_PickServer(t *testing.T) {
	type selectorFn func() (memcache.ServerSelector, error)

	type memcacheHost struct {
		Host   string
		Weight int
	}

	memcacheHosts := []memcacheHost{
		{"10.20.0.15:11211", 1},
		{"10.20.0.17:11211", 1},
		{"10.20.0.18:11211", 1},
	}

	keys := []string{
		"model_model:billing_wallet_v3:1318485",
		"model_model:billing_wallet_v3:1318486",
		"model_model:billing_wallet_v3:1318487",
		"model_model:productCategory_v9:3936242",
		"model_model:image_v1:idBySite:829945:byType:27",
		"model_model:productCategory_v9:4323570",
		"model_Dklab_Cache_Backend_TagEmuWrapper_1.50_:image_v1:4:50727804",
		"model_model:text:1206988",
		"model_model:user:552214",
	}

	tests := []struct {
		name    string
		fn      selectorFn
		wantErr bool
	}{
		{
			name: "github.com/bradfitz/gomemcache/memcache",
			fn: func() (memcache.ServerSelector, error) {
				sl := &memcache.ServerList{}

				var servers []string
				for _, v := range memcacheHosts {
					servers = append(servers, v.Host)
				}

				if err := sl.SetServers(servers...); err != nil {
					return nil, err
				}

				return sl, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - md5",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return md5.New()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - md5 with weight",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr:   addr,
						Memory: uint64(v.Weight),
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return md5.New()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - crc32IEEE",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return crc32.NewIEEE()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - crc32Castagnoli",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return crc32.New(crc32.MakeTable(crc32.Castagnoli))
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - crc32Koopman",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return crc32.New(crc32.MakeTable(crc32.Koopman))
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - crc64ISO",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return crc64.New(crc64.MakeTable(crc64.ISO))
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - crc64ECMA",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return crc64.New(crc64.MakeTable(crc64.ECMA))
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv32",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New32()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv32a",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New32a()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv64",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New64()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv64a",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New64a()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv128",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New128()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv128 with weight",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr:   addr,
						Memory: uint64(v.Weight),
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New128()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv128a",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr: addr,
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New128a()
				})

				return continuum, nil
			},
		},
		{
			name: "github.com/rckclmbr/goketama/ketama - fnv128a with weight",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr:   addr,
						Memory: uint64(v.Weight),
					})
				}
				continuum := goketama.New(servers, func() hash.Hash {
					return fnv.New128a()
				})

				return continuum, nil
			},
		},
		// {
		// 	name: "github.com/liyinhgqw/memcache_client",
		// 	fn: func() (memcache.ServerSelector, error) {
		// 		sl := &memcache_client.ServerList{}

		// 		var servers []string
		// 		for _, v := range memcacheHosts {
		// 			servers = append(servers, v.Host)
		// 		}

		// 		if err := sl.SetServers(servers...); err != nil {
		// 			return nil, err
		// 		}

		// 		// sl.(memcache.ServerSelector)

		// 		return sl.(memcache.ServerSelector), nil
		// 	},
		// },
		{
			name: "github.com/dgryski/go-ketama",
			fn: func() (memcache.ServerSelector, error) {
				var buckets []go_ketama.Bucket
				for _, v := range memcacheHosts {

					buckets = append(buckets, go_ketama.Bucket{
						Label:  v.Host,
						Weight: v.Weight,
					})
				}

				continuum := dgryski.New(buckets)

				return continuum, nil
			},
		},
		{
			name: "github.com/dgryski/go-ketama with hash func",
			fn: func() (memcache.ServerSelector, error) {
				var buckets []go_ketama.Bucket
				for _, v := range memcacheHosts {

					buckets = append(buckets, go_ketama.Bucket{
						Label:  v.Host,
						Weight: v.Weight,
					})
				}

				continuum := dgryski.NewWithHash(buckets, go_ketama.HashFunc2)

				return continuum, nil
			},
		},
		{
			name: "github.com/ngerakines/ketama",
			fn: func() (memcache.ServerSelector, error) {
				var servers []goketama.ServerInfo
				for _, v := range memcacheHosts {
					addr, err := goketama.ServerAddr(v.Host)
					if err != nil {
						return nil, err
					}
					servers = append(servers, goketama.ServerInfo{
						Addr:   addr,
						Memory: uint64(v.Weight),
					})
				}
				continuum := ngerakines.New(servers, 100)

				return continuum, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss, err := tt.fn()
			if err != nil {
				t.Errorf("ServerList.PickServer() error = %v", err)
			}

			for _, key := range keys {
				got, err := ss.PickServer(key)
				if (err != nil) != tt.wantErr {
					t.Errorf("ServerList.PickServer() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				fmt.Printf("\t%s - %s\n", got.String(), key)
			}
		})
	}
}
