package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

func ShowDatabases(db *sql.Conn) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW DATABASES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

// ShowTables shows the tables of a database, the caller should use the correct database.
func ShowTables(db *sql.Conn) ([]string, error) {
	var res oneStrColumnTable
	if err := simpleQuery(db, "SHOW TABLES", res.handleOneRow); err != nil {
		return nil, err
	}
	return res.data, nil
}

func ShowCreateDatabase(db *sql.Conn, database string) (string, error) {
	var oneRow [2]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1])
	}
	query := fmt.Sprintf("SHOW CREATE DATABASE `%s`", escapeString(database))
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", errors.WithMessage(err, query)
	}
	return oneRow[1], nil
}

func ShowCreateTable(db *sql.Conn, database, table string) (string, error) {
	var oneRow [2]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1])
	}
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", escapeString(database), escapeString(table))
	err := simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", errors.WithMessage(err, query)
	}
	return oneRow[1], nil
}

func ShowCreateView(db *sql.Conn, database, view string) (string, string, error) {
	var fieldNames []string
	handleFieldRow := func(rows *sql.Rows) error {
		var oneRow [6]sql.NullString
		err := rows.Scan(&oneRow[0], &oneRow[1], &oneRow[2], &oneRow[3], &oneRow[4], &oneRow[5])
		if err != nil {
			return err
		}
		if oneRow[0].Valid {
			fieldNames = append(fieldNames, fmt.Sprintf("`%s` int", escapeString(oneRow[0].String)))
		}
		return nil
	}
	var oneRow [4]string
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(&oneRow[0], &oneRow[1], &oneRow[2], &oneRow[3])
	}
	var createTableSQL, createViewSQL strings.Builder

	// Build createTableSQL
	query := fmt.Sprintf("SHOW FIELDS FROM `%s`.`%s`", escapeString(database), escapeString(view))
	err := simpleQuery(db, query, handleFieldRow)
	if err != nil {
		return "", "", errors.WithMessage(err, query)
	}
	fmt.Fprintf(&createTableSQL, "CREATE TABLE `%s`(\n", escapeString(view))
	createTableSQL.WriteString(strings.Join(fieldNames, ",\n"))
	createTableSQL.WriteString("\n)ENGINE=MyISAM;\n")

	// Build createViewSQL
	fmt.Fprintf(&createViewSQL, "DROP TABLE IF EXISTS `%s`;\n", escapeString(view))
	fmt.Fprintf(&createViewSQL, "DROP VIEW IF EXISTS `%s`;\n", escapeString(view))
	query = fmt.Sprintf("SHOW CREATE VIEW `%s`.`%s`", escapeString(database), escapeString(view))
	err = simpleQuery(db, query, handleOneRow)
	if err != nil {
		return "", "", errors.WithMessage(err, query)
	}
	// The result for `show create view` SQL
	// mysql> show create view v1;
	// +------+-------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
	// | View | Create View                                                                                                                         | character_set_client | collation_connection |
	// +------+-------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
	// | v1   | CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v1` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t` | utf8                 | utf8_general_ci      |
	// +------+-------------------------------------------------------------------------------------------------------------------------------------+----------------------+----------------------+
	SetCharset(&createViewSQL, oneRow[2], oneRow[3])
	createViewSQL.WriteString(oneRow[1])
	createViewSQL.WriteString(";\n")
	RestoreCharset(&createViewSQL)

	return createTableSQL.String(), createViewSQL.String(), nil
}

func SetCharset(w *strings.Builder, characterSet, collationConnection string) {
	w.WriteString("SET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\n")
	w.WriteString("SET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\n")
	w.WriteString("SET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\n")

	fmt.Fprintf(w, "SET character_set_client = %s;\n", characterSet)
	fmt.Fprintf(w, "SET character_set_results = %s;\n", characterSet)
	fmt.Fprintf(w, "SET collation_connection = %s;\n", collationConnection)
}

func RestoreCharset(w *strings.Builder) {
	w.WriteString("SET character_set_client = @PREV_CHARACTER_SET_CLIENT;\n")
	w.WriteString("SET character_set_results = @PREV_CHARACTER_SET_RESULTS;\n")
	w.WriteString("SET collation_connection = @PREV_COLLATION_CONNECTION;\n")
}

func ListAllDatabasesTables(db *sql.Conn, databaseNames []string, tableType TableType) (DatabaseTables, error) {
	var tableTypeStr string
	switch tableType {
	case TableTypeBase:
		tableTypeStr = "BASE TABLE"
	case TableTypeView:
		tableTypeStr = "VIEW"
	default:
		return nil, errors.Errorf("unknown table type %v", tableType)
	}

	query := fmt.Sprintf("SELECT table_schema,table_name FROM information_schema.tables WHERE table_type = '%s'", tableTypeStr)
	dbTables := DatabaseTables{}
	for _, schema := range databaseNames {
		dbTables[schema] = make([]*TableInfo, 0)
	}

	if err := simpleQueryWithArgs(db, func(rows *sql.Rows) error {
		var schema, table string
		if err := rows.Scan(&schema, &table); err != nil {
			return withStack(err)
		}

		// only append tables to schemas in databaseNames
		if _, ok := dbTables[schema]; ok {
			dbTables[schema] = append(dbTables[schema], &TableInfo{table, tableType})
		}
		return nil
	}, query); err != nil {
		return nil, errors.WithMessage(err, query)
	}
	return dbTables, nil
}

func ListAllTables(db *sql.Conn, database string) ([]string, error) {
	var tables oneStrColumnTable
	const query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? and table_type = 'BASE TABLE'"
	if err := simpleQueryWithArgs(db, tables.handleOneRow, query, database); err != nil {
		return nil, errors.WithMessage(err, query)
	}
	return tables.data, nil
}

func ListAllViews(db *sql.Conn, database string) ([]string, error) {
	var views oneStrColumnTable
	const query = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? and table_type = 'VIEW'"
	if err := simpleQueryWithArgs(db, views.handleOneRow, query, database); err != nil {
		return nil, errors.WithMessage(err, query)
	}
	return views.data, nil
}

func SelectVersion(db *sql.DB) (string, error) {
	var versionInfo string
	const query = "SELECT version()"
	row := db.QueryRow(query)
	err := row.Scan(&versionInfo)
	if err != nil {
		return "", withStack(errors.WithMessage(err, query))
	}
	return versionInfo, nil
}

func SelectAllFromTable(conf *Config, db *sql.Conn, database, table string) (TableDataIR, error) {
	selectedField, err := buildSelectField(db, database, table, conf.CompleteInsert)
	if err != nil {
		return nil, err
	}

	colTypes, err := GetColumnTypes(db, selectedField, database, table)
	if err != nil {
		return nil, err
	}

	orderByClause, err := buildOrderByClause(conf, db, database, table)
	if err != nil {
		return nil, err
	}

	query := buildSelectQuery(database, table, selectedField, buildWhereCondition(conf, ""), orderByClause)

	return &tableData{
		database:        database,
		table:           table,
		query:           query,
		colTypes:        colTypes,
		selectedField:   selectedField,
		escapeBackslash: conf.EscapeBackslash,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, nil
}

func SelectFromSql(conf *Config, db *sql.Conn) (TableDataIR, error) {
	log.Info("dump data from sql", zap.String("sql", conf.Sql))
	rows, err := db.QueryContext(context.Background(), conf.Sql)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, conf.Sql))
	}
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, withStack(errors.WithMessage(err, conf.Sql))
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, withStack(errors.WithMessage(err, conf.Sql))
	}
	for i := range cols {
		cols[i] = wrapBackTicks(cols[i])
	}
	return &tableData{
		database:        "",
		table:           "",
		rows:            rows,
		colTypes:        colTypes,
		selectedField:   strings.Join(cols, ","),
		escapeBackslash: conf.EscapeBackslash,
		specCmts: []string{
			"/*!40101 SET NAMES binary*/;",
		},
	}, nil
}

func buildSelectQuery(database, table string, fields string, where string, orderByClause string) string {
	var query strings.Builder
	query.WriteString("SELECT ")
	query.WriteString(fields)
	query.WriteString(" FROM `")
	query.WriteString(escapeString(database))
	query.WriteString("`.`")
	query.WriteString(escapeString(table))
	query.WriteString("`")

	if where != "" {
		query.WriteString(" ")
		query.WriteString(where)
	}

	if orderByClause != "" {
		query.WriteString(" ")
		query.WriteString(orderByClause)
	}

	return query.String()
}

func buildOrderByClause(conf *Config, db *sql.Conn, database, table string) (string, error) {
	if !conf.SortByPk {
		return "", nil
	}
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		ok, err := SelectTiDBRowID(db, database, table)
		if err != nil {
			return "", withStack(err)
		}
		if ok {
			return "ORDER BY _tidb_rowid", nil
		} else {
			return "", nil
		}
	}
	pkName, err := GetPrimaryKeyName(db, database, table)
	if err != nil {
		return "", withStack(err)
	}
	tableContainsPriKey := pkName != ""
	if tableContainsPriKey {
		return fmt.Sprintf("ORDER BY `%s`", escapeString(pkName)), nil
	}
	return "", nil
}

func SelectTiDBRowID(db *sql.Conn, database, table string) (bool, error) {
	const errBadFieldCode = 1054
	tiDBRowIDQuery := fmt.Sprintf("SELECT _tidb_rowid from `%s`.`%s` LIMIT 0", escapeString(database), escapeString(table))
	_, err := db.ExecContext(context.Background(), tiDBRowIDQuery)
	if err != nil {
		errMsg := strings.ToLower(err.Error())
		if strings.Contains(errMsg, fmt.Sprintf("%d", errBadFieldCode)) {
			return false, nil
		}
		return false, withStack(errors.WithMessage(err, tiDBRowIDQuery))
	}
	return true, nil
}

func GetColumnTypes(db *sql.Conn, fields, database, table string) ([]*sql.ColumnType, error) {
	query := fmt.Sprintf("SELECT %s FROM `%s`.`%s` LIMIT 1", fields, escapeString(database), escapeString(table))
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}
	defer rows.Close()
	return rows.ColumnTypes()
}

func GetPrimaryKeyName(db *sql.Conn, database, table string) (string, error) {
	priKeyQuery := "SELECT column_name FROM information_schema.columns " +
		"WHERE table_schema = ? AND table_name = ? AND column_key = 'PRI';"
	var colName string
	row := db.QueryRowContext(context.Background(), priKeyQuery, database, table)
	if err := row.Scan(&colName); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return "", withStack(errors.WithMessage(err, priKeyQuery))
		}
	}
	return colName, nil
}

func GetUniqueIndexName(db *sql.Conn, database, table string) (string, error) {
	uniKeyQuery := "SELECT column_name FROM information_schema.columns " +
		"WHERE table_schema = ? AND table_name = ? AND column_key = 'UNI';"
	var colName string
	row := db.QueryRowContext(context.Background(), uniKeyQuery, database, table)
	if err := row.Scan(&colName); err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return "", withStack(errors.WithMessage(err, uniKeyQuery))
		}
	}
	return colName, nil
}

func FlushTableWithReadLock(ctx context.Context, db *sql.Conn) error {
	const ftwrlQuery = "FLUSH TABLES WITH READ LOCK"
	_, err := db.ExecContext(ctx, ftwrlQuery)
	return withStack(errors.WithMessage(err, ftwrlQuery))
}

func LockTables(ctx context.Context, db *sql.Conn, database, table string) error {
	lockTableQuery := fmt.Sprintf("LOCK TABLES `%s`.`%s` READ", escapeString(database), escapeString(table))
	_, err := db.ExecContext(ctx, lockTableQuery)
	return withStack(errors.WithMessage(err, lockTableQuery))
}

func UnlockTables(ctx context.Context, db *sql.Conn) error {
	const unlockTableQuery = "UNLOCK TABLES"
	_, err := db.ExecContext(ctx, unlockTableQuery)
	return withStack(errors.WithMessage(err, unlockTableQuery))
}

func UseDatabase(db *sql.DB, databaseName string) error {
	_, err := db.Exec(fmt.Sprintf("USE `%s`", escapeString(databaseName)))
	return withStack(err)
}

func ShowMasterStatus(db *sql.Conn, fieldNum int) ([]string, error) {
	oneRow := make([]string, fieldNum)
	addr := make([]interface{}, fieldNum)
	for i := range oneRow {
		addr[i] = &oneRow[i]
	}
	handleOneRow := func(rows *sql.Rows) error {
		return rows.Scan(addr...)
	}
	const showMasterStatusQuery = "SHOW MASTER STATUS"
	err := simpleQuery(db, showMasterStatusQuery, handleOneRow)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, showMasterStatusQuery))
	}
	return oneRow, nil
}

func GetSpecifiedColumnValue(rows *sql.Rows, columnName string) ([]string, error) {
	var strs []string
	columns, _ := rows.Columns()
	addr := make([]interface{}, len(columns))
	oneRow := make([]sql.NullString, len(columns))
	var fieldIndex = -1
	for i, col := range columns {
		if strings.ToUpper(col) == columnName {
			fieldIndex = i
		}
		addr[i] = &oneRow[i]
	}
	if fieldIndex == -1 {
		return strs, nil
	}
	for rows.Next() {
		err := rows.Scan(addr...)
		if err != nil {
			return strs, err
		}
		strs = append(strs, oneRow[fieldIndex].String)
	}
	return strs, nil
}

func GetPdAddrs(db *sql.DB) ([]string, error) {
	query := "SELECT * FROM information_schema.cluster_info where type = 'pd';"
	rows, err := db.Query(query)
	if err != nil {
		log.Warn("can't execute query from db",
			zap.String("query", query), zap.Error(err))
		return []string{}, withStack(errors.WithMessage(err, query))
	}
	defer rows.Close()
	return GetSpecifiedColumnValue(rows, "STATUS_ADDRESS")
}

func GetTiDBDDLIDs(db *sql.DB) ([]string, error) {
	query := "SELECT * FROM information_schema.tidb_servers_info;"
	rows, err := db.Query(query)
	if err != nil {
		log.Warn("can't execute query from db",
			zap.String("query", query), zap.Error(err))
		return []string{}, withStack(errors.WithMessage(err, query))
	}
	defer rows.Close()
	return GetSpecifiedColumnValue(rows, "DDL_ID")
}

func DecodeTiDBClusteredRegionKeysToJSON(db *sql.Conn, regionKeys []string) []map[string]interface{} {
	query := "SELECT tidb_decode_row_key(?);"
	var ret []map[string]interface{}
	for _, key := range regionKeys {
		var decodeResult string
		row, err := db.QueryContext(context.Background(), query, key)
		if err != nil {
			log.Info("unable to decode region key, skip", zap.String("key", key))
			continue
		}
		err = row.Scan(&decodeResult)
		if err != nil || key == decodeResult {
			log.Info("unable to decode region key, skip", zap.String("key", key))
			continue
		}
		var js map[string]interface{}
		err = json.Unmarshal([]byte(decodeResult), &js)
		if err != nil {
			log.Warn("unable to unmarshal json when decoding TiDB region key, skip",
				zap.String("key", key), zap.String("decode result", decodeResult))
			continue
		}
		switch v := js["handle"].(type) {
		case string:
			// table id not found, fallback to old method.
			log.Warn("unable to decode the origin value when decoding TiDB region key, skip",
				zap.String("key", key), zap.String("decode result", decodeResult), zap.String("handle", v))
			continue
		case map[string]interface{}:
		default:
			log.Warn("unexpected json value type when decoding TiDB region key",
				zap.String("key", key), zap.String("decode result", decodeResult), zap.Any("handle", v))
		}
		ret = append(ret, js)
	}
	return ret
}

func GetTiDBRegionStartKeys(db *sql.Conn, dbName, tableName string) []string {
	query := "SELECT start_key FROM information_schema.tikv_region_status s, information_schema.tables t " +
		"WHERE s.table_id = t.tidb_table_id AND t.db_name = ? AND t.table_name = ? AND is_index = 0 ORDER BY start_key;"
	rows, err := db.QueryContext(context.Background(), query, dbName, tableName)
	if err != nil {
		log.Info("can't execute query from db",
			zap.String("query", query), zap.Error(err))
		return []string{}
	}
	defer rows.Close()
	startKeys, err := GetSpecifiedColumnValue(rows, "START_KEY")
	if err != nil {
		log.Warn("can't get specified column value START_KEY for query",
			zap.String("query", query), zap.Error(err))
	}
	return startKeys
}

func CheckTiDBWithTiKV(db *sql.DB) (bool, error) {
	var count int
	query := "SELECT COUNT(1) as c FROM MYSQL.TiDB WHERE VARIABLE_NAME='tikv_gc_safe_point'"
	row := db.QueryRow(query)
	err := row.Scan(&count)
	if err != nil {
		return false, withStack(errors.WithMessage(err, query))
	}
	return count > 0, nil
}

func getSnapshot(db *sql.Conn) (string, error) {
	str, err := ShowMasterStatus(db, showMasterStatusFieldNum)
	if err != nil {
		return "", err
	}
	return str[snapshotFieldIndex], nil
}

func isUnknownSystemVariableErr(err error) bool {
	return strings.Contains(err.Error(), "Unknown system variable")
}

func resetDBWithSessionParams(db *sql.DB, dsn string, params map[string]interface{}) (*sql.DB, error) {
	support := make(map[string]interface{})
	for k, v := range params {
		s := fmt.Sprintf("SET SESSION %s = ?", k)
		_, err := db.Exec(s, v)
		if err != nil {
			if isUnknownSystemVariableErr(err) {
				log.Info("session variable is not supported by db", zap.String("variable", k), zap.Reflect("value", v))
				continue
			}
			return nil, withStack(err)
		}

		support[k] = v
	}

	for k, v := range support {
		var s string
		if str, ok := v.(string); ok {
			s = wrapStringWith(str, "'")
		} else {
			s = fmt.Sprintf("%v", v)
		}
		dsn += fmt.Sprintf("&%s=%s", k, url.QueryEscape(s))
	}

	newDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, withStack(err)
	}

	db.Close()

	return newDB, nil
}

func createConnWithConsistency(ctx context.Context, db *sql.DB) (*sql.Conn, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, withStack(err)
	}
	query := "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	_, err = conn.ExecContext(ctx, query)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}
	query = "START TRANSACTION /*!40108 WITH CONSISTENT SNAPSHOT */"
	_, err = conn.ExecContext(ctx, query)
	if err != nil {
		return nil, withStack(errors.WithMessage(err, query))
	}
	return conn, nil
}

func buildSelectField(db *sql.Conn, dbName, tableName string, completeInsert bool) (string, error) {
	query := `SELECT COLUMN_NAME,EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?;`
	rows, err := db.QueryContext(context.Background(), query, dbName, tableName)
	if err != nil {
		return "", withStack(errors.WithMessage(err, query))
	}
	defer rows.Close()
	availableFields := make([]string, 0)

	hasGenerateColumn := false
	var fieldName string
	var extra string
	for rows.Next() {
		err = rows.Scan(&fieldName, &extra)
		if err != nil {
			return "", withStack(errors.WithMessage(err, query))
		}
		switch extra {
		case "STORED GENERATED", "VIRTUAL GENERATED":
			hasGenerateColumn = true
			continue
		}
		availableFields = append(availableFields, wrapBackTicks(escapeString(fieldName)))
	}
	if completeInsert || hasGenerateColumn {
		return strings.Join(availableFields, ","), nil
	}
	return "*", nil
}

type oneStrColumnTable struct {
	data []string
}

func (o *oneStrColumnTable) handleOneRow(rows *sql.Rows) error {
	var str string
	if err := rows.Scan(&str); err != nil {
		return withStack(err)
	}
	o.data = append(o.data, str)
	return nil
}

func simpleQuery(conn *sql.Conn, sql string, handleOneRow func(*sql.Rows) error) error {
	return simpleQueryWithArgs(conn, handleOneRow, sql)
}

func simpleQueryWithArgs(conn *sql.Conn, handleOneRow func(*sql.Rows) error, sql string, args ...interface{}) error {
	rows, err := conn.QueryContext(context.Background(), sql, args...)
	if err != nil {
		return withStack(errors.WithMessage(err, sql))
	}
	defer rows.Close()

	for rows.Next() {
		if err := handleOneRow(rows); err != nil {
			rows.Close()
			return withStack(errors.WithMessage(err, sql))
		}
	}
	rows.Close()
	return rows.Err()
}

func pickupPossibleField(dbName, tableName string, db *sql.Conn, conf *Config) (string, error) {
	// If detected server is TiDB, try using _tidb_rowid
	if conf.ServerInfo.ServerType == ServerTypeTiDB {
		ok, err := SelectTiDBRowID(db, dbName, tableName)
		if err != nil {
			return "", nil
		}
		if ok {
			return "_tidb_rowid", nil
		}
	}
	// try to use pk
	fieldName, err := GetPrimaryKeyName(db, dbName, tableName)
	if err != nil {
		return "", err
	}
	// try to use first uniqueIndex
	if fieldName == "" {
		fieldName, err = GetUniqueIndexName(db, dbName, tableName)
		if err != nil {
			return "", err
		}
	}

	// there is no proper index
	if fieldName == "" {
		return "", nil
	}

	query := "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
		"WHERE TABLE_NAME = ? AND COLUMN_NAME = ?"
	var fieldType string
	row := db.QueryRowContext(context.Background(), query, tableName, fieldName)
	err = row.Scan(&fieldType)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		} else {
			return "", withStack(errors.WithMessage(err, query))
		}
	}
	switch strings.ToLower(fieldType) {
	case "int", "bigint":
		return fieldName, nil
	}
	return "", nil
}

func estimateCount(dbName, tableName string, db *sql.Conn, field string, conf *Config) uint64 {
	query := fmt.Sprintf("EXPLAIN SELECT `%s` FROM `%s`.`%s`", field, escapeString(dbName), escapeString(tableName))

	if conf.Where != "" {
		query += " WHERE "
		query += conf.Where
	}

	estRows := detectEstimateRows(db, query, []string{"rows", "estRows", "count"})
	/* tidb results field name is estRows (before 4.0.0-beta.2: count)
		+-----------------------+----------+-----------+---------------------------------------------------------+
		| id                    | estRows  | task      | access object | operator info                           |
		+-----------------------+----------+-----------+---------------------------------------------------------+
		| tablereader_5         | 10000.00 | root      |               | data:tablefullscan_4                    |
		| └─tablefullscan_4     | 10000.00 | cop[tikv] | table:a       | table:a, keep order:false, stats:pseudo |
		+-----------------------+----------+-----------+----------------------------------------------------------

	mariadb result field name is rows
		+------+-------------+---------+-------+---------------+------+---------+------+----------+-------------+
		| id   | select_type | table   | type  | possible_keys | key  | key_len | ref  | rows     | Extra       |
		+------+-------------+---------+-------+---------------+------+---------+------+----------+-------------+
		|    1 | SIMPLE      | sbtest1 | index | NULL          | k_1  | 4       | NULL | 15000049 | Using index |
		+------+-------------+---------+-------+---------------+------+---------+------+----------+-------------+

	mysql result field name is rows
		+----+-------------+-------+------------+-------+---------------+-----------+---------+------+------+----------+-------------+
		| id | select_type | table | partitions | type  | possible_keys | key       | key_len | ref  | rows | filtered | Extra       |
		+----+-------------+-------+------------+-------+---------------+-----------+---------+------+------+----------+-------------+
		|  1 | SIMPLE      | t1    | NULL       | index | NULL          | multi_col | 10      | NULL |    5 |   100.00 | Using index |
		+----+-------------+-------+------------+-------+---------------+-----------+---------+------+------+----------+-------------+
	*/
	if estRows > 0 {
		return estRows
	}
	return 0
}

func detectEstimateRows(db *sql.Conn, query string, fieldNames []string) uint64 {
	row, err := db.QueryContext(context.Background(), query)
	if err != nil {
		log.Warn("can't execute query from db",
			zap.String("query", query), zap.Error(err))
		return 0
	}
	defer row.Close()
	row.Next()
	columns, _ := row.Columns()
	addr := make([]interface{}, len(columns))
	oneRow := make([]sql.NullString, len(columns))
	var fieldIndex = -1
	for i := range oneRow {
		addr[i] = &oneRow[i]
	}
found:
	for i := range oneRow {
		for _, fieldName := range fieldNames {
			if strings.EqualFold(columns[i], fieldName) {
				fieldIndex = i
				break found
			}
		}
	}
	err = row.Scan(addr...)
	if err != nil || fieldIndex < 0 {
		log.Warn("can't get estimate count from db",
			zap.String("query", query), zap.Error(err))
		return 0
	}

	estRows, err := strconv.ParseFloat(oneRow[fieldIndex].String, 64)
	if err != nil {
		log.Warn("can't get parse rows from db",
			zap.String("query", query), zap.Error(err))
		return 0
	}
	return uint64(estRows)
}

func parseSnapshotToTSO(pool *sql.DB, snapshot string) (uint64, error) {
	snapshotTS, err := strconv.ParseUint(snapshot, 10, 64)
	if err == nil {
		return snapshotTS, nil
	}
	var tso sql.NullInt64
	query := "SELECT unix_timestamp(?)"
	row := pool.QueryRow(query, snapshot)
	err = row.Scan(&tso)
	if err != nil {
		return 0, withStack(errors.WithMessage(err, strings.ReplaceAll(query, "?", fmt.Sprintf(`"%s"`, snapshot))))
	}
	if !tso.Valid {
		return 0, withStack(fmt.Errorf("snapshot %s format not supported. please use tso or '2006-01-02 15:04:05' format time", snapshot))
	}
	return (uint64(tso.Int64)<<18)*1000 + 1, nil
}

func buildWhereCondition(conf *Config, where string) string {
	var query strings.Builder
	separator := "WHERE"
	if conf.Where != "" {
		query.WriteString(" ")
		query.WriteString(separator)
		query.WriteString(" ")
		query.WriteString(conf.Where)
		separator = "AND"
	}
	if where != "" {
		query.WriteString(" ")
		query.WriteString(separator)
		query.WriteString(" ")
		query.WriteString(where)
	}
	return query.String()
}

func escapeString(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}
