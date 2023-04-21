# LikeMatcher - A MySQL Compatible SQL Like Clause filter
The goal of this project is to build a Golang like matcher that is fully compatible with MySQL syntax, easy to extend, and high performance.

## How to use it

### Import Dependencies
```shell
go get -v github.com/gotodb/gotodb/pkg/likematcher
import "github.com/gotodb/gotodb/pkg/likematcher"
```
### Filter
```go
package main

import (
	"fmt"

	"github.com/gotodb/gotodb/pkg/likematcher"
)


func main() {
	like := "%test%"
	escape := ""
	matcher, err := Compile(like, escape)

	if err != nil {
		panic(err)
	}

	ss := []string{"test", "1test", "1test1", "tes"}
	for _, s := range ss {
		if matcher.Match([]byte(s)) {
			fmt.Printf("match %s\n", s)
		} else {
			fmt.Printf("not match %s\n", s)
		}
	}
}
```

Test the matcher by running the following command:

```shell
go run main.go
```

If the matcher runs properly, you should get a result like this:

```text
match test
match 1test
match 1test1
not match tes
```

## Future

- Support dense dfa matcher

## Contributing

Contributions are welcomed and greatly appreciated.
[Issue](https://github.com/gotodb/gotodb/issues/new) 

## License

Parser is under the GPLv3 license. See the LICENSE file for details.