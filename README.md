# gomemcache selectors

-   [Memcache consistent hash portable for Go](https://github.com/iMega/gomemcache-selector/blob/main/php-memcache/selector.go)
-   [Memcache consistent hash](https://github.com/php/pecl-caching-memcache/blob/master/memcache_consistent_hash.c)

## Using

```
$ go get github.com/imega/gomemcache-selector/php-memcache
```

## Example

```
import (
        "github.com/bradfitz/gomemcache/memcache"
        "github.com/imega/gomemcache-selector/php-memcache"
)

func main() {
    selector := phpmemcache.New("10.0.0.1:11211", "10.0.0.2:11211", "10.0.0.3:11212")
    mc := memcache.NewFromSelector(selector)

    mc.Set(&memcache.Item{Key: "foo", Value: []byte("my value")})

    it, err := mc.Get("foo")
    ...
}
```

## Reference

-   [Simple hash memcache client](https://github.com/bradfitz/gomemcache)
-   [A gomemcache ServerSelector using the ketama selection algorithm ](https://github.com/rckclmbr/goketama)
-   [Consistent Hashing: Algorithmic Tradeoffs - Damian Gryski](https://dgryski.medium.com/consistent-hashing-algorithmic-tradeoffs-ef6b8e2fcae8)
-   [Ketama implementation compatible with Algorithm::ConsistentHash::Ketama](github.com/dgryski/go-ketama)
-   [libketama-style consistent hashing in Go](https://github.com/ngerakines/ketama)
