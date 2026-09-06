package repairnzb

import (
	"crypto/rand"
)

// msgIDAlphabet has exactly 32 symbols, so each 5-bit slice of a random byte
// maps to one character without modulo bias.
const msgIDAlphabet = "abcdefghijklmnopqrstuvwxyz234567"

const (
	msgIDLocalLen  = 32
	msgIDDomainLen = 8
	msgIDTLDLen    = 3
)

// generateRandomMessageID builds a globally unique Message-ID for a reposted
// article.
//
// The randomness is cryptographic: message IDs are minted concurrently by every
// upload worker, and a clock-seeded generator collides whenever two workers land
// in the same tick, which silently overwrites an article on the server.
func generateRandomMessageID() string {
	const total = msgIDLocalLen + msgIDDomainLen + msgIDTLDLen

	raw := make([]byte, total)
	// crypto/rand.Read is documented never to return an error.
	_, _ = rand.Read(raw)

	out := make([]byte, 0, total+2)
	for i, b := range raw {
		if i == msgIDLocalLen {
			out = append(out, '@')
		}

		if i == msgIDLocalLen+msgIDDomainLen {
			out = append(out, '.')
		}

		out = append(out, msgIDAlphabet[b&0x1f])
	}

	return string(out)
}
