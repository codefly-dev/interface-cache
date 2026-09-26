package cache

import "context"

// EntryKey is the layer key s currently stores its copy of key under in p, so
// tests can inspect a layer directly. It never mints a generation.
func EntryKey(s *Stack, p Partition, key string) string {
	if s.origin == nil {
		return p.layerKey(key)
	}
	generation := ""
	if len(s.tiers) > 0 {
		e, err := s.read(context.Background(), fill{layer: generationKey(key)})
		if err != nil {
			return ""
		}
		generation = string(e.Value)
	}
	return p.entryKey(generation, key)
}
