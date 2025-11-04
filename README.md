# RDB Parser

This library is capable of parsing RDB files upto version 11.

It can also be used to parse and dump single RDB values, and verify RDB files and values.

## API

### Parsing a file

The following code demonstrates how to parse an RDB file.

Note that, only the database with the number 0 is parsed, and the rest
is skipped or an error is raised, depending on the handler implementation.

The same holds true for some types of metadata or function definition in
the RDB file.

```go
import (
	"log"

	"github.com/upstash/rdb"
)

type fileHandler struct {
}

// Implement rdb.FileHandler methods, which will be called
// as the file is parsed.

func main() {
	err := rdb.ReadFile("/path/to/dump.rdb", &fileHandler{})
	if err != nil {
		log.Fatal(err)
	}
}
```

### Parsing a value

The following code demonstrates how to parse a single RDB value.

```go
import (
	"log"

	"github.com/upstash/rdb"
)

type valueHandler struct {
}

// Implement rdb.ValueHandler methods, in which one of them will
// called depending on the type of the value in the given payload.

func main() {
	payload := []byte{ /*RDB value payload*/ }
	err := rdb.ReadValue("key", payload, &valueHandler{})
	if err != nil {
		log.Fatal(err)
	}
}
```

### Dumping a value

The following code demonsrates how to dump a single RDB value as a payload
that can be parsed with `rdb.ReadValue`.

```go
import (
	"github.com/upstash/rdb"
)

func main() {
	writer := rdb.NewWriter()
	writer.WriteType(rdb.TypeString)
	writer.WriteString("foo")
	writer.WriteChecksum(rdb.Version)
	writer.GetBuffer() // payload
}
```

### Verifying a file

The following code demonstrates how to verify an RDB file is not corrupt, and
does not exceed the defined limits of the total data, max entry, and max key sizes.

```go
import (
	"log"

	"github.com/upstash/rdb"
)

func main() {
	opts := rdb.VerifyFileOptions{
		MaxDataSize:  256 << 20, // 256 MB
		MaxEntrySize: 100 << 20, // 100 MB
		MaxStreamPELSize: 1000,
	}
	err := rdb.VerifyFile("/path/to/dump.rdb", opts)
	if err != nil {
		log.Fatal(err)
	}
}
```

### Verifying a reader

The following code demonstrates how to verify an io.Reader to reads an RDB file is not corrupt, and
does not exceed the defined limits of the total data, max entry, and max key sizes.

```go
import (
	"io"
	"log"

	"github.com/upstash/rdb"
)

func main() {
	opts := rdb.VerifyReaderOptions{
		MaxDataSize:  256 << 20, // 256 MB
		MaxEntrySize: 100 << 20, // 100 MB
		MaxStreamPELSize: 1000,
	}
	
	var r io.Reader
	// initialize reader
	
	err := rdb.VerifyReader(r, opts)
	if err != nil {
		log.Fatal(err)
	}
}
```

### Verifying a value

The following code demonstrates how to verify an RDB value is not corrupt, and
does not exceed the defined limits of the max entry size.

```go
import (
	"log"

	"github.com/upstash/rdb"
)

func main() {
	opts := rdb.VerifyValueOptions{
		MaxEntrySize: 100 << 20, // 100 MB
		MaxStreamPELSize: 1000,
	}
	payload := []byte{ /*RDB value payload*/ }
	err := rdb.VerifyValue(payload, opts)
	if err != nil {
		log.Fatal(err)
	}
}
```
