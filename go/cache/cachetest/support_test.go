package cachetest

import "testing"

// A scenario that waits for "the reader heard the notice" goes on to read. The
// recorder must say so only once the stack has applied the notice (evicted the
// copies above, voided the fills racing it); said earlier, the read lands in
// the delivery-to-application window and is rightly served the old copy.
func TestRecorderHearsANoticeOnlyOnceTheStackHasAppliedIt(t *testing.T) {
	r := &recorder{}
	applying, applied := make(chan struct{}), make(chan struct{})
	notify := r.notified(func(string) {
		close(applying)
		<-applied
	})
	done := make(chan struct{})
	go func() { notify("k"); close(done) }()

	<-applying
	if r.heardAll([]string{"k"}) {
		t.Fatal("the recorder reported a notice heard while the stack was still applying it")
	}
	close(applied)
	<-done
	if !r.heardAll([]string{"k"}) {
		t.Fatal("the recorder did not report a notice heard after the stack applied it")
	}
}
