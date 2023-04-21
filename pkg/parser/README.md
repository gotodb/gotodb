# Parser - A MySQL Compatible SQL Parser

The goal of this project is to build a Golang parser that is fully compatible with MySQL syntax, easy to extend, and high performance. Currently, features supported by parser are as follows:

- Highly compatible with MySQL: it supports almost all features of MySQL.
- Extensible: adding a new syntax requires only a few lines of antlr and Golang code changes.
## How to use it

### Import Dependencies
```shell
go get -v github.com/gotodb/gotodb/pkg/parser
import "github.com/gotodb/gotodb/pkg/parser"
```
### Filter
```go
package main

import (
	"fmt"

	"github.com/antlr/antlr4/runtime/Go/antlr/v4"
	"github.com/gotodb/gotodb/pkg/parser"
)


func main() {
	sqlStr := "SHOW TABLES"
	inputStream := antlr.NewInputStream(sqlStr)
	lexer := parser.NewSqlLexer(parser.NewCaseChangingStream(inputStream, true))
	p := parser.NewSqlParser(antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel))
	errListener := parser.NewErrorListener()
	p.AddErrorListener(errListener)
	tree := p.SingleStatement()
	if errListener.HasError() {
		panic(errListener)
	} else {
		fmt.Printf("%v", tree)
	}
}
```

Test the parser by running the following command:

```shell
go run main.go
```

If the parser runs properly, you should get a result like this:

```text
&{BaseParserRuleContext:0xc0001886e0 parser:0xc00014bc20}
```

## Future

- Support more MySQL syntax
- Improve the quality of code and comments

## Contributing

Contributions are welcomed and greatly appreciated.
[Issue](https://github.com/gotodb/gotodb/issues/new) 

## License

Parser is under the GPLv3 license. See the LICENSE file for details.
