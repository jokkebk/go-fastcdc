package fastcdc

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func fillLCG(data []byte, seed uint32) {
	const m = 1 << 31
	const a = 1103515245
	const c = 12345

	for i := range data {
		seed = (a*seed + c) % m
		// Will repeat after 16 MB (1 << 24)
		data[i] = byte((seed >> 16) & 0xFF)
	}
}

func TestChunker(t *testing.T) {
	// Generate 1 MB of pseudorandom data
	data := make([]byte, 1*miB)
	_, err := rand.Read(data)
	if err != nil {
		t.Fatalf("failed to generate random data: %v", err)
		return
	}

	fillLCG(data, 42)

	// Create a Chunker instance
	reader := bytes.NewReader(data)
	chunker := NewChunkerWithParams(reader, 8*kiB, 32*kiB, 128*kiB)

	// Expected offsets
	expectedOffsets := []int{
		36714, 59235, 100431, 133475, 183955, 227175, 262536, 331968,
		367735, 418065, 450929, 504275, 555138, 588843, 645038, 684445,
		720786, 745512, 783877, 828354, 871489, 906239, 945918, 982639,
		1007331, 1043460, 1048576,
	}

	// Collect actual offsets
	var actualOffsets []int
	for {
		offset, err := chunker.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("error getting next chunk: %v", err)
		}
		actualOffsets = append(actualOffsets, offset)
	}

	// Verify the generated offsets
	if len(actualOffsets) != len(expectedOffsets) {
		t.Fatalf("expected %d offsets, got %d", len(expectedOffsets), len(actualOffsets))
	}

	for i, offset := range actualOffsets {
		if offset != expectedOffsets[i] {
			t.Errorf("expected offset %d at index %d, got %d", expectedOffsets[i], i, offset)
		}
	}
}
