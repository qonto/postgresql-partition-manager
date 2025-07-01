// Package uuid7 provides function to generate UUIDv7 from time
package uuid7

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"
)

//nolint:mnd
func FromTime(timestamp time.Time) string {
	// Convert timestamp to Unix time in milliseconds
	unixMillis := timestamp.UnixNano() / int64(time.Millisecond)

	// Create a byte slice from the Unix time (48 bits, big endian)
	// Ensure the slice is initially 8 bytes to accommodate the full uint64,
	// but we'll only use the last 6 bytes for the timestamp
	timeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timeBytes, uint64(unixMillis)) //nolint:gosec
	timeBytes = timeBytes[2:]                                 // Keep the last 6 bytes

	// Generate random bytes for the rest of the UUID (10 bytes to make it a total of 16)
	randomBytes := make([]byte, 10)

	_, err := rand.Read(randomBytes)
	if err != nil {
		panic("Failed to generate random bytes")
	}

	// Combine time bytes and random bytes
	uuidBytes := append(timeBytes, randomBytes...) //nolint:gocritic,makezero

	// Encode the UUID bytes to a string
	uuid := fmt.Sprintf("%x-%x-7000-0000-000000000000", uuidBytes[0:4], uuidBytes[4:6])

	return uuid
}
