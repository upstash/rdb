# RDB Parser

This library is capable of parsing RDB files upto version 11.

It can also be used to parse and write single RDB values.

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
	payload := []byte{/*RDB value payload*/}
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
