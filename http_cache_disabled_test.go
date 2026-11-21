package ravendb

import "testing"

func TestHttpCacheDisabledStoresNothing(t *testing.T) {
	cache := newHttpCache(0)
	changeVector := "A:1"
	cache.set("/docs?id=a", &changeVector, []byte(`{}`))
	cache.setNotFound("/docs?id=b")

	if got := cache.GetNumberOfItems(); got != 0 {
		t.Fatalf("items = %d, want 0", got)
	}
	item, gotVector, payload := cache.get("/docs?id=a")
	if item.item != nil || gotVector != nil || payload != nil {
		t.Fatalf("get returned a cached response")
	}
}

func TestHttpCacheEnabledStoresResponse(t *testing.T) {
	cache := newHttpCache(1024)
	changeVector := "A:1"
	cache.set("/docs?id=a", &changeVector, []byte(`{}`))

	_, gotVector, _ := cache.get("/docs?id=a")
	if gotVector == nil || *gotVector != changeVector {
		t.Fatalf("change vector = %v, want %q", gotVector, changeVector)
	}
}

func TestSetMaxHttpCacheSizeDisablesExecutorCache(t *testing.T) {
	conventions := NewDocumentConventions()
	conventions.SetMaxHttpCacheSize(0)
	if !newHttpCache(conventions.getMaxHttpCacheSize()).disabled {
		t.Fatalf("cache enabled with size 0")
	}
}

func TestHttpCacheDisabledByDefault(t *testing.T) {
	if !newHttpCache(NewDocumentConventions().getMaxHttpCacheSize()).disabled {
		t.Fatalf("cache enabled with default conventions")
	}
}
