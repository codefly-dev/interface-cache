package cache

import "testing"

func TestPartitionKeys(t *testing.T) {
	tenant := NewPartition("tenant")
	for _, c := range []struct{ got, want string }{
		{tenant.layerKey("k"), "p6:tenant:k"},
		{NewPartition("tenant", WriteAround()).layerKey("k"), "p6:tenant:k"},
		{Global().layerKey("k"), "g:k"},
		{tenant.entryKey("gen", "k"), "P6:tenant:3:gen:k"},
		{Global().entryKey("gen", "k"), "G3:gen:k"},
		{generationKey("k"), "i:k"},
	} {
		if c.got != c.want {
			t.Fatalf("layer key %q, want %q", c.got, c.want)
		}
	}
	// No split of partition, generation and key reaches another's entry.
	if NewPartition("x").entryKey("g", "y:z") == NewPartition("x:y").entryKey("g", "z") ||
		NewPartition("x").entryKey("g:1", "k") == NewPartition("x").entryKey("g", "1:k") ||
		NewPartition("x").layerKey("y:z") == NewPartition("x:y").layerKey("z") {
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
