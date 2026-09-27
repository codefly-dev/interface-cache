package cache

import "testing"

func TestPartitionKeys(t *testing.T) {
	tenant := NewPartition("tenant")
	for _, c := range []struct{ got, want string }{
		{tenant.layerKey("", "k"), "3p0::6:tenant:k"},
		{tenant.layerKey("v2", "k"), "3p2:v2:6:tenant:k"},
		{NewPartition("tenant", WriteAround()).layerKey("", "k"), "3p0::6:tenant:k"},
		{Global().layerKey("", "k"), "3g0::k"},
		{tenant.entryKey("", "gen", "k"), "3P0::6:tenant:3:gen:k"},
		{Global().entryKey("s", "gen", "k"), "3G1:s:3:gen:k"},
		{generationKey("k"), "3i:k"},
	} {
		if c.got != c.want {
			t.Fatalf("layer key %q, want %q", c.got, c.want)
		}
	}
	// No split of schema, partition, generation and key reaches another's
	// entry.
	if NewPartition("x").entryKey("", "g", "y:z") == NewPartition("x:y").entryKey("", "g", "z") ||
		NewPartition("x").entryKey("", "g:1", "k") == NewPartition("x").entryKey("", "g", "1:k") ||
		NewPartition("x").entryKey("s", "g", "k") == NewPartition("x").entryKey("", "g", "k") ||
		NewPartition("x").layerKey("", "y:z") == NewPartition("x:y").layerKey("", "z") ||
		NewPartition("x").layerKey("a:1", "k") == NewPartition("x").layerKey("a", "1:k") {
		t.Fatal("layer keys are ambiguous")
	}
	a, again, granted := NewPartition("a"), NewPartition("a"), NewPartition("a", WriteAround())
	if a != again || a == granted {
		t.Fatal("partitions do not compare by key and write-around")
	}
	if a, b := newGeneration(), newGeneration(); a == b || len(a) != 24 {
		t.Fatalf("generations %q and %q are not fresh 24-character tokens", a, b)
	}
}

// keyOf reads the cache key back from every family, whatever the parts before
// it contain.
func TestKeyOfLayerKey(t *testing.T) {
	for _, key := range []string{"k", "a:b", "3i:x", "", "12:x:"} {
		for _, layer := range []string{
			NewPartition("p:1").layerKey("s:2", key),
			Global().layerKey("", key),
			NewPartition("p").entryKey("", "g:3", key),
			Global().entryKey("s", "gen", key),
			generationKey(key),
		} {
			if got := keyOf(layer); got != key {
				t.Fatalf("keyOf(%q) = %q, want %q", layer, got, key)
			}
		}
	}
	if got := keyOf("cache:v:{x}"); got != "cache:v:{x}" {
		t.Fatalf("a foreign key parsed as %q", got)
	}
}
