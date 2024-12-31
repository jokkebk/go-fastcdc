# go-fastcdc

FastCDC (Fast Content-Defined Chunking) implementation in Go.

## Overview

This library implements the FastCDC content-defined chunking algorithm, which splits data into variable-sized chunks based on content. The implementation provides:

- Configurable minimum, average and maximum chunk sizes 
- Efficient streaming chunking with buffered reading
- Default chunking parameters optimized for general use

## Usage

```go
import "github.com/jokkebk/go-fastcdc"

// Create a chunker with default parameters (2KB min, 8KB avg, 32KB max)
chunker := fastcdc.NewChunker(reader)

// Or customize chunk size parameters
chunker := fastcdc.NewChunkerWithParams(reader, 2*1024, 8*1024, 32*1024)

// Get chunk boundaries
for {
    offset, err := chunker.Next() 
    if err == io.EOF {
        break
    }
    // Process chunk ending at offset
}
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details