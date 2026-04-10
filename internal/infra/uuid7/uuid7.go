// Package uuid7 provides function to generate UUIDv7 from time per RFC 9562
package uuid7

import (
	"encoding/binary"
	"fmt"
	"time"
)

// FromTime generates a deterministic UUIDv7 from the given timestamp.
// The rand_a and rand_b fields are zeroed; only the version (7) and
// variant (RFC 9562, 0b10) bits are set.
//
// Layout (RFC 9562 §5.7):
//
//	Bits  0-47:  unix_ts_ms  (48-bit big-endian milliseconds since Unix epoch)
//	Bits 48-51:  ver         (0b0111 = 7)
//	Bits 52-63:  rand_a      (zeroed)
//	Bits 64-65:  var         (0b10)
//	Bits 66-127: rand_b      (zeroed)
//
//nolint:mnd
func FromTime(timestamp time.Time) string {
	unixMillis := timestamp.UnixNano() / int64(time.Millisecond)

	var buf [16]byte

	// Bytes 0-5: 48-bit unix_ts_ms (big-endian)
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(unixMillis))
	copy(buf[0:6], ts[2:])

	// Byte 6: upper nibble = version 0b0111, lower nibble = rand_a[0:3] (zero)
	buf[6] = 0x70

	// Byte 8: variant 0b10 in the two MSBs (RFC 9562 §4.1)
	buf[8] = 0x80

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}
