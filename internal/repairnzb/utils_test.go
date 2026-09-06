package repairnzb

import (
	"strings"
	"sync"
	"testing"
)

func TestGenerateRandomMessageIDShape(t *testing.T) {
	id := generateRandomMessageID()

	local, rest, ok := strings.Cut(id, "@")
	if !ok {
		t.Fatalf("id %q has no @", id)
	}

	domain, tld, ok := strings.Cut(rest, ".")
	if !ok {
		t.Fatalf("id %q has no . after @", id)
	}

	if len(local) != msgIDLocalLen {
		t.Errorf("local part %q: got len %d; want %d", local, len(local), msgIDLocalLen)
	}
	if len(domain) != msgIDDomainLen {
		t.Errorf("domain %q: got len %d; want %d", domain, len(domain), msgIDDomainLen)
	}
	if len(tld) != msgIDTLDLen {
		t.Errorf("tld %q: got len %d; want %d", tld, len(tld), msgIDTLDLen)
	}

	for _, r := range local + domain + tld {
		if !strings.ContainsRune(msgIDAlphabet, r) {
			t.Errorf("id %q contains %q, outside the alphabet", id, r)
		}
	}
}

// TestGenerateRandomMessageIDConcurrentUniqueness guards the collision bug: the
// previous implementation seeded a fresh RNG from the wall clock per call, so
// concurrent callers within one tick produced identical IDs.
func TestGenerateRandomMessageIDConcurrentUniqueness(t *testing.T) {
	const (
		workers   = 16
		perWorker = 500
	)

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		ids = make(map[string]struct{}, workers*perWorker)
	)

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			local := make([]string, 0, perWorker)
			for range perWorker {
				local = append(local, generateRandomMessageID())
			}

			mu.Lock()
			defer mu.Unlock()

			for _, id := range local {
				if _, dup := ids[id]; dup {
					t.Errorf("duplicate message ID generated: %s", id)
				}
				ids[id] = struct{}{}
			}
		}()
	}

	wg.Wait()

	if len(ids) != workers*perWorker {
		t.Errorf("got %d unique IDs; want %d", len(ids), workers*perWorker)
	}
}
