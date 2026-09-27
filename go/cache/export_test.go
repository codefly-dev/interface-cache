package cache

import "context"

// EntryKey is the layer key s currently stores its copy of key under in p, so
// tests can inspect a layer directly. It never mints a generation.
func EntryKey(s *Stack, p Partition, key string) string {
	if s.origin == nil {
		return p.layerKey(s.schema, key)
	}
	generation := ""
	if len(s.tiers) > 0 {
		f := s.newFill(generationKey(key), s.since(), s.mint)
		f.generation = true
		e, err := s.read(context.Background(), s.mode, f)
		if err != nil {
			return ""
		}
		generation = string(e.Value)
	}
	return p.entryKey(s.schema, generation, key)
}
