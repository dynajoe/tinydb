# TinyDB

## Overview
A relational database engine that implements a subset of ANSI SQL.

### Purpose
This project came to be as a learning exercise for digging into implementation details of database engines. Ultimately, the learnings from making TinyDB will be used to construct a class or series of conference talks on implementation details. A major benefit of writing a database engine that’s not production worthy is that it is exponentially easier to grok what’s going on and improve the learning experience.

### Capabilities

* Go SQL Driver over net.Conn
* TCP server
* SQL Queries
  * Parsing
  * Transactions
  * Prepared statements
  * Raw
* Virtual machine with simple instruction set and unlimited (in theory) registers
* BTree binary file format compatible with SQLite

### Future

* Programmability similar to that of plpgsql or plv8.

## Running
### Configuration
### TCP Server
### CLI

## Internals
### Parsing
The query parser uses a set of simple parser combinators. The advantage of this approach is arguably its simplicity. The drawback is exponential time complexity in worst case. With the addition of "checkpoints" the amortized time complexity is polynomial.

#### Lexer / Tokenizer
Input is broken into tokens that consist of one or more characters except EOF which is zero. For example:

tsqlString: everything between two `'`'s. e.g. `'a simple string'` would produce a token containing the text `a simple string`.

tsqlSelect: SELECT in any case. e.g. SeLeCT.

tsqlBoolean: A token with true or false.

#### Scanner

#### Parser combinators

Some of the combinators defined in this project:

**required**: only succeeds if the parser succeeds otherwise, no input is consumed and the parser fails.

**optional**: always succeeds and may consume input if the parser succeeds.

**all**: requires that all parsers succeed or no input in consumed.

**zeroOrMore**: runs parser until it doesn't match anymore and always succeeds.

**separatedBy1**: this combinator is useful for parsing comma separated lists.

**oneOf**: executes each parser until a success. one parser must succeed.

**lazy**: calls a parser producing function each time it's invoked. This combinator is useful when a parser refers to itself.

**chainl**: requires at least one expression followed by an optional series of [op expression]
this combinator is used to eliminate left recursion and build a left-associative expression.

Left recursion:
e.g. Expression -> Expression + Term
pseudo code: (would never terminate)

```
void Expression() {
  Expression();
  match('+');
  Term();
}
```

left-to-right recursive descent parsers can't handle left recursion and this is just one of several possible
ways to eliminate left recursion. Though, this particular way doesn't handle indirect left recurision which
is a series of substitutions that ultimately lead to an infinite recursive call.

Left associativity:
e.g. (from wikipedia) Consider the expression a ~ b ~ c.
If the operator ~ has left associativity, this expression would be interpreted as (a ~ b) ~ c.
If the operator has right associativity, the expression would be interpreted as a ~ (b ~ c).

#### AST
#### Descriptive errors
### Storage
#### Binary format
#### Internal/Leaf Pages
### Virtual Machine
#### OpCodes
#### Code Generation
#### Optimizer

## Bibliography

SQLite file format
ChiDB architecture
