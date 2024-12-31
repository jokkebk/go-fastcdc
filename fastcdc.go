package fastcdc

import (
	"io"
)

type Chunker struct {
	reader io.Reader
	eof    bool // Whether we've hit EOF

	buf       []byte
	bufOffset int // Offset of buffer start in reader
	pos       int // Current position in buffer
	available int // Number of bytes available in buffer

	minSize int
	avgSize int
	maxSize int

	maskS uint64
	maskL uint64
}

const (
	kiB = 1024
	miB = 1024 * kiB
)

func NewChunker(reader io.Reader) *Chunker {
	return NewChunkerWithParams(reader, 2*kiB, 8*kiB, 32*kiB)
}

func NewChunkerWithParams(reader io.Reader, minSize, avgSize, maxSize int) *Chunker {
	b := bits(avgSize) - 1
	maskS := spread(b + 2)
	maskL := spread(b - 2)
	return &Chunker{
		reader:  reader,
		buf:     make([]byte, maxSize*2),
		minSize: minSize,
		avgSize: avgSize,
		maxSize: maxSize,
		maskS:   maskS,
		maskL:   maskL,
	}
}

// fillBuffer attempts to fill the buffer with data from the reader
// It returns an error if encountered during reading
func (c *Chunker) fillBuffer() error {
	// If buffer is full, no need to fill
	if c.available == len(c.buf) {
		return nil
	}

	// Keep reading until buffer is full or EOF
	for !c.eof && c.available < len(c.buf) {
		n, err := c.reader.Read(c.buf[c.available:])
		c.available += n
		if err == io.EOF {
			c.eof = true
			return nil
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// Next returns the offset of next chunk boundary
func (c *Chunker) Next() (int, error) {
	// If we don't have enough data in the buffer to potentially find a cut point
	if !c.eof && c.available-c.pos < c.maxSize {
		// Move any remaining data to start of buffer
		if c.pos > 0 {
			copy(c.buf, c.buf[c.pos:c.available])
			c.bufOffset += c.pos // advance buffer offset
			c.available -= c.pos // adjust available data
			c.pos = 0
		}

		// Try to fill the buffer
		if err := c.fillBuffer(); err != nil {
			return 0, err
		}
	}

	//fmt.Printf("at %d, pos: %d, available: %d\n", c.bufOffset, c.pos, c.available)

	// If we have no data left, we're done
	if c.pos >= c.available {
		return 0, io.EOF
	}

	// Find cut point -- can also be size of available data (if EOF)
	cutPoint := c.findCutPoint(c.buf[c.pos:c.available])

	// Update positions
	c.pos += cutPoint

	// Return offset of cut point
	return c.bufOffset + c.pos, nil
}

// findCutPoint implements the FastCDC cut point selection algorithm
func (c *Chunker) findCutPoint(data []byte) int {
	//fmt.Printf("findCutPoint(%d), %d\n", len(data), data[0])

	if len(data) <= c.minSize {
		//fmt.Printf("data length %d <= minSize %d\n", len(data), c.minSize)
		return len(data)
	}

	// Initialize fingerprint
	fp := uint64(0)
	i := c.minSize

	// Search using the "small" mask between min and avg size
	for ; i < c.avgSize && i < len(data); i++ {
		fp = (fp << 1) + G[data[i]]
		if (fp & c.maskS) == 0 {
			//fmt.Printf("maskS cut point at %d (between %d and %d)\n", i, c.minSize, c.avgSize)
			return i
		}
	}

	// Search using the "large" mask if we haven't found a cut point
	for ; i < c.maxSize && i < len(data); i++ {
		fp = (fp << 1) + G[data[i]]
		if (fp & c.maskL) == 0 {
			//fmt.Printf("maskL cut point at %d (between %d and %d)\n", i, c.avgSize, c.maxSize)
			return i
		}
	}

	//fmt.Printf("no cut point found, returning %d\n", i)
	// If we haven't found a cut point, return max size or end of data
	return i
}

// bits returns the number of bits needed to represent n
func bits(n int) int {
	var i int
	for ; n > 0; i++ {
		n >>= 1
	}
	return i
}

// Spread N bits over 64 bits
// For example, if n = 8, we want to spread 8 bits over 64 bits
// This means there will be 64-8 = 56 bits of padding zeros,
// spread evenly between 8 ones -- that is 8-1 = 7 gaps
func spread(n int) uint64 {
	shift := (64-n)/(n-1) + 1

	mask := uint64(1)

	for i := 0; i < n-1; i++ {
		mask = (mask << shift) + 1
	}

	return mask
}

var G = [256]uint64{
	0x92df698b0712f6a9, 0x178890f5c6e263fd, 0x2ea2d3133b84c892, 0xa6017137d1c2dae1,
	0x40edfd7586018f38, 0x33b726290f9d0d6, 0x20a88f2695ab1609, 0xd814dce8c05cb7e1,
	0x5d97ef891e039acd, 0xa223ea673da3b21f, 0x6d0b95dc28d1318d, 0xea00d1839f060e2f,
	0x8e13739522fc2075, 0xe24dd2ced3dd88d7, 0x31ea9a99591d5a3a, 0x466ccec4dc7bd20,
	0x46712f3f65fb823b, 0xbdec9a33f459dfe3, 0xdfeb23a1a5c3df44, 0xa21fa52e8608fa57,
	0x915ff8409d472345, 0xa5f5c2ca645bfefe, 0x913c72a8b4bd7d46, 0x57cf17101a492c8c,
	0x86a9d29aae11b1ba, 0xacc5d3f102ae1681, 0xa7ba99c9e2d76e7e, 0xefe6f7f426718b75,
	0xd1ee472238a53aa8, 0xad7ea5ccddf8dfde, 0xadfa042ebb6a4347, 0x790c691651969218,
	0xe8f3d7ff5f2c7ed8, 0x54c30adcc9c7f6eb, 0x741ee0dd51102a5b, 0xb480b88be0689ac,
	0xcd3e82343b2d108a, 0xc3aa7825d3fe252e, 0xae2e65f17d1ca124, 0x5fb68edf21c3c92,
	0x7ccd6734440d32d4, 0xea9c66495ac08d5f, 0x3ef1497db30c5461, 0x5e10b29fb0a10125,
	0xdf92fef1b8f91fb2, 0x5f481ef47b90ecb3, 0x62c1b8746e28a2e2, 0x5a77382d92c70e10,
	0x8af6a2fea0192e71, 0xa8183bef97df3b32, 0xc649fde1d7eda6de, 0x28c897595a614024,
	0xc7581ad5eda5b70e, 0xcb86cfbbdd47a5c9, 0x9c5e952c12f5e6e0, 0xbe075f9a8c05e6c9,
	0x1cca496ff2a106af, 0x94645016fa05c633, 0x604837aa7577e9f, 0xef2130160acaa372,
	0xe1531735e1376ae6, 0x73759264a9d2c43c, 0x2154141fdcfcca5e, 0x4564ccdcf2798540,
	0xc1ecf896702ee299, 0xf46c2458bbd0056d, 0x553ba26b9de2d796, 0x9d426230864ae017,
	0xb5bd00c51e1e677c, 0xe028b4eed6c4f58f, 0x59fce411f787650c, 0xbed44a3ed6a57fe4,
	0x1c119ec3766958fd, 0x8570c6bd73618358, 0x336785656d98e979, 0xdc8fecc0807d02f0,
	0xe2c9c96bff744e63, 0xf0428d2f8d8c6bda, 0x769011abda256da1, 0x1a1b46895de768f0,
	0xb0b7bd268620525c, 0x840875cc0934d09f, 0x295bc0a6832cafc6, 0x19c19698fc302292,
	0xa604ecbcee1cf24a, 0x8f0ff10a79c3b7fd, 0xab2f04a9a56bf1d5, 0xbe3ca346295c7e78,
	0xefc7b55402690e73, 0xf95202fcea2c3f0f, 0x23d404639f3e5868, 0xd4b0e352238d532b,
	0xd413c09421d5369, 0xb02d8b7f8e06381c, 0xa966986190f42ac5, 0x76ad584e88645788,
	0x409139b1911423cf, 0x1e650a45502c4b54, 0x5ca66dee8ccbac32, 0xd651cb1b0164a708,
	0x71dedec18adc3621, 0xbc7a56741a757a83, 0xeedfaf02355485a9, 0x4d39fff8d991e4f7,
	0xc697d5ecea29ba4d, 0xb4cda25e9bb0f8d6, 0x892d2647326309af, 0xfeed97c1a66ca850,
	0xc5a7235d4b96550c, 0x218c4f7f81d9e5c7, 0x825b79c0eabd4e9, 0x932c6c399a10d8cf,
	0xe08e720864d2687f, 0x6a8a0cebc023b2c2, 0x15f1fd034d80ef57, 0x4087605d6de2fd04,
	0x1057f6d23a4d5445, 0xee4d369f1e9d6ff2, 0x62456bc02003bc9c, 0x69c30c225d89f0e7,
	0xa8eac2e44043a40a, 0xfb4a7d76742e9a67, 0xeb1696dfbcb37f95, 0x66a4fc44ddc0ec02,
	0xa5ed09d7681a2d8d, 0xfaa6093b351a6caf, 0x860322b00af6038, 0xe5a5482fbc7d8252,
	0xdc312ad8628522a6, 0x8c5ec483e805bde6, 0x80f96a44465cf9a, 0xbf3ee40c9aaf0b79,
	0x77eb20a4af4ac8dd, 0xbba0f124325dfc1a, 0x92eaeec1b2022aa0, 0x44cf0836abd9b051,
	0xececb7a2fe551885, 0xa8440b6c283a6c50, 0x8d063420d70be54b, 0x95e573bfac1205b7,
	0xd39f95a856a48790, 0x128cb2b6f3e04d01, 0xf57c51e6d2b9b583, 0x73a774593f1ccb17,
	0x58cf3d179c305fe, 0x1fda886e4b7dca25, 0x7f78ea99d792d438, 0xa3d6d2717f8e5fd4,
	0x5082ee968e6f9dd6, 0x57cb8b40c4b8606f, 0x48838a3637714e43, 0xc9c8dd91d267381e,
	0xf81273f32462d81e, 0x7b2e8c4b733d974e, 0xd94c844b0b56ed6c, 0x306490c0eb0a6520,
	0xc94dbdde85479855, 0xf41e983ad8cd9aa4, 0xf9d65e3a263d19f8, 0x59c5938a47b6296d,
	0x3ca49312b953c0, 0x594d9fbeaf2ab7ee, 0x3794da21ae3b48eb, 0xd9ef0adbecc29858,
	0x7fed6a27a0c2bafc, 0x378c3a293840d6ae, 0x7a28141a018d88c2, 0xbf58b4abd0b7447b,
	0x70621e7cab4c5227, 0x30692ac609cfb536, 0xaebc00ad6a09580, 0x31c15c7694bcf163,
	0xd64fc13916fff451, 0x6677a9e425e863e1, 0x7da5b2600ad4e231, 0x502406ab15fc58bd,
	0x78a9f8b2f7bb54ac, 0x6e04ba15aefabf59, 0xe0fef445488ff7e6, 0x6bd1e59260bef8f6,
	0x33b66e71da95be5d, 0xa58226b5d0b69e5a, 0x34a46f56ff3f9026, 0x67ac5bf005c5e3e0,
	0xb8d9dcdc76eb1c9, 0x1cbd7a233366c51d, 0x3705053a1e409569, 0xc5b5a12769e1f1cf,
	0x8880ff82837aa93c, 0x888e5e1ba077ab20, 0xf6c77ee680610512, 0x7b7aa934b41b512f,
	0x6c20fc4408cb2cb8, 0x2551d75b09489471, 0xbb13925e9400849a, 0x2f2c9ee7b91aa793,
	0xdc3108bbf27571a3, 0x3515939a8f008003, 0x1d52cf73358b1aff, 0xda76edf46c2aa4f8,
	0xc3c197f9c1ad5b58, 0x5fb7b510a324b53d, 0x4bdf31cbc1b19d4b, 0xdc2c8a46658af8c7,
	0x367d04483640cf0d, 0x57842fc2ae378048, 0xa1faf8437eb4a991, 0xb103fcc09afa20b8,
	0x61fae538169c6e, 0xb8fd69f85ec4650a, 0x6b973ffa055651f9, 0xab15b2f130f40a07,
	0xf9310eea2c224f09, 0x5a939a46ee03911c, 0x1a15044dba5312ba, 0x8b302affb36e1429,
	0x34fa799200c1e36b, 0xedad4fd9880570d7, 0x80d64669ed1236bf, 0xb4188afc434b52d5,
	0x30423844ac06a617, 0x6d9f7534b0873cfb, 0xb8545058f624c612, 0x657350deb769fb4,
	0x117572f27911b4e3, 0x9b6ea784634a2458, 0x2fc9046a17194528, 0x79f8378e55fab62e,
	0xf668a5c35e6c7516, 0x4ee0d82f96b7b69a, 0xc934b98fcd1ecede, 0xe004507797fec721,
	0xf23d33caadd18fe3, 0x7c1cd63c4b532aaa, 0xa6a36ff73c67597d, 0xa2b3aaf4254ebd9f,
	0x8f0703c5dd2238c3, 0x83cdeef8c24668bc, 0xd3290f2880906dd6, 0xc627760cc7e4d0d1,
	0x21b3908a1fd13921, 0x587a8c693f26ca95, 0x15d41978cd759bb1, 0x36bc328590bf60a8,
	0x5a31b094bf44d83e, 0x6d4d771720dcc15a, 0xa107d2883a4ca512, 0x467d0ddab63d56f4,
	0xab04c709459fb85a, 0xedf47470fdec1454, 0x4cc3b47b4c6e5d67, 0x1c6a9777826cebf7,
	0xd898c57a08f79fd, 0x6ccac1f8d716845f, 0xb3def1b20dad4483, 0xf82dba3c0b540201,
}
