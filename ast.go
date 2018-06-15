package main

// OperatorPrecedenceLookup provides precedence for operators
// https://docs.microsoft.com/en-us/sql/t-sql/language-elements/operator-precedence-transact-sql?view=sql-server-2017
func OperatorPrecedenceLookup() map[Token]int {
	return map[Token]int{
		tsqlEquals: 4,
		tsqlGt:     4,
		tsqlLt:     4,
		tsqlLte:    4,
		tsqlGte:    4,
		tsqlNotEq:  4,
		tsqlNot:    5,
		tsqlAnd:    6,
		tsqlOr:     7,
	}
}
