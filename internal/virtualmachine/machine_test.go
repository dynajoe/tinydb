package virtualmachine

// type VMTestSuite struct {
// 	suite.Suite
// 	tempDir string
// 	engine  *engine.Engine
// }

// func (s *VMTestSuite) SetupTest() {
// 	tempDir, err := ioutil.TempDir(os.TempDir(), "tinydb")
// 	s.tempDir = tempDir
// 	if err != nil {
// 		s.Error(err)
// 	}
// 	s.engine = engine.Start(&engine.Config{
// 		DataDir:           tempDir,
// 		UseVirtualMachine: true,
// 	})
// }

// func (s *VMTestSuite) TearDownTest() {
// 	if s.tempDir != "" {
// 		_ = os.RemoveAll(s.tempDir)
// 	}
// }

// func TestVMTestSuite(t *testing.T) {
// 	suite.Run(t, new(VMTestSuite))
// }

// func (s *VMTestSuite) TestSimple() {
// 	sql := "select * from foo"
// 	instructions := []instruction{
// 		{OpInteger, 1, 0, 0, 0},
// 		{OpString, len(sql), 1, x, sql},
// 		{OpNull, 0, 2, 0, 0},
// 		{OpResultRow, 0, 3, 0, 0},
// 		{OpInteger, 2, 0, 0, 0},
// 		{OpResultRow, 0, 3, 0, 0},
// 		{OpInteger, 3, 0, 0, 0},
// 		{OpResultRow, 0, 3, 0, 0},
// 		{OpInteger, 4, 0, 0, 0},
// 		{OpResultRow, 0, 3, 0, 0},
// 		{OpInteger, 5, 0, 0, 0},
// 		{OpResultRow, 0, 3, 0, 0},
// 		{OpHalt, 0, 0, 0, 0},
// 	}

// 	testProgram := NewProgram(s.engine, instructions)
// 	go testProgram.Run()

// 	type testrow struct {
// 		id   int
// 		sql  string
// 		null interface{}
// 	}

// 	var results []testrow
// outer:
// 	for {
// 		select {
// 		case <-time.After(time.Second):
// 			s.Fail("row timeout")
// 		case r := <-testProgram.Results():
// 			if r == nil {
// 				break outer
// 			}
// 			results = append(results, testrow{
// 				id:   r[0].(int),
// 				sql:  r[1].(string),
// 				null: r[2],
// 			})
// 		}
// 	}

// 	s.Len(results, 5)
// 	for i, r := range results {
// 		s.Equal(i+1, r.id)
// 		s.Equal(sql, r.sql)
// 		s.Nil(r.null)
// 	}
// }

// func (s *VMTestSuite) TestCreateTable() {
// 	createSQL := "CREATE TABLE company (company_id int PRIMARY KEY, company_name text);"
// 	stmt, err := tsql.Parse(createSQL)
// 	s.NoError(err)

// 	createTableStatement, ok := stmt.(*ast.CreateTableStatement)
// 	s.True(ok)

// 	instructions := CreateTableInstructions(createTableStatement)
// 	program := NewProgram(s.engine, instructions)
// 	program.Run()

// 	tableDefinition, err := s.engine.GetTableDefinition("company")
// 	s.NoError(err)
// 	s.Len(tableDefinition.Columns, 2)
// }

// func (s *VMTestSuite) TestInsert() {
// 	_, err := interpret.Execute(s.engine, "CREATE TABLE company (company_id int PRIMARY KEY, company_name text, description text);")
// 	s.NoError(err)

// 	insertSQL := "INSERT INTO company (company_id, company_name, description) VALUES (99, 'hashicorp', NULL)"
// 	stmt, err := tsql.Parse(insertSQL)
// 	s.NoError(err)

// 	insertStmt, ok := stmt.(*ast.InsertStatement)
// 	s.True(ok)

// 	instructions := InsertInstructions(s.engine, insertStmt)
// 	program := NewProgram(s.engine, instructions)
// 	err = program.Run()
// 	s.NoError(err)

// 	// Ensure the row was inserted
// 	result, err := interpret.Execute(s.engine, "SELECT * FROM company")
// 	s.NoError(err)
// 	select {
// 	case <-time.After(time.Second):
// 		s.Fail("test timeout")
// 	case row := <-result.Rows:
// 		s.EqualValues(row.Data[0], 99)
// 		s.Equal(row.Data[1], "hashicorp")
// 		s.Nil(row.Data[2])
// 	}
// }

// func (s *VMTestSuite) TestSelect() {
// 	_, err := interpret.Execute(s.engine, "CREATE TABLE company (company_id int PRIMARY KEY, company_name text, description text);")
// 	s.NoError(err)
// 	_, err = interpret.Execute(s.engine, "INSERT INTO company (company_id, company_name, description) VALUES (99, 'hashicorp', NULL)")
// 	s.NoError(err)

// 	stmt, err := tsql.Parse("SELECT * FROM company")
// 	s.NoError(err)
// 	selectStmt, ok := stmt.(*ast.SelectStatement)
// 	s.True(ok)

// 	instructions := SelectInstructions(s.engine, selectStmt)
// 	program := NewProgram(s.engine, instructions)
// 	go program.Run()

// 	select {
// 	case <-time.After(time.Second):
// 		s.Fail("test timeout")
// 	case row := <-program.Results():
// 		if row == nil {
// 			s.FailNow("expected a row")
// 		}
// 		s.EqualValues(row[0], 99)
// 		s.Equal(row[1], "hashicorp")
// 		s.Nil(row[2])
// 	}
// }
