package mysqlmock

import (
	"context"
	"database/sql"
	"strings"
)

type sqliteResultSet struct {
	Columns []resultColumn
	rows    *sql.Rows
	scan    []any
}

func (c *mysqlConn) querySQLite(ctx context.Context, query string, args ...any) (resultSet, error) {
	resp, err := c.querySQLiteText(ctx, query, false, args...)
	if err != nil {
		return resultSet{}, err
	}
	switch v := resp.(type) {
	case resultSet:
		return v, nil
	case *sqliteResultSet:
		return v.materialize()
	default:
		return resultSet{}, errPacket(mysqlErrUnknown, "HY000", "Unsupported SQLite query response")
	}
}

func (c *mysqlConn) querySQLiteText(ctx context.Context, query string, stream bool, args ...any) (any, error) {
	rows, err := c.sqliteConn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	columns, streamable, err := c.sqliteResultColumns(query, rows)
	if err != nil {
		_ = rows.Close()
		return nil, err
	}

	rs := &sqliteResultSet{Columns: columns, rows: rows}
	if stream && streamable {
		return rs, nil
	}
	return rs.materialize()
}

func (c *mysqlConn) sqliteResultColumns(query string, rows *sql.Rows) ([]resultColumn, bool, error) {
	names, err := rows.Columns()
	if err != nil {
		return nil, false, err
	}
	columnTypes, _ := rows.ColumnTypes()
	columns := make([]resultColumn, len(names))
	zeroFillWidths := c.resultZeroFillWidths(query, names)
	streamable := true
	for i, name := range names {
		columns[i] = resultColumn{Name: name, Type: fieldTypeVarString}
		if i < len(columnTypes) {
			dbType := columnTypes[i].DatabaseTypeName()
			columns[i].Type = mysqlFieldType(columnTypes[i])
			columns[i].ZeroFillWidth = mysqlZeroFillWidth(dbType)
			if strings.TrimSpace(dbType) == "" {
				streamable = false
			}
		} else {
			streamable = false
		}
		if i < len(zeroFillWidths) && zeroFillWidths[i] > columns[i].ZeroFillWidth {
			columns[i].ZeroFillWidth = zeroFillWidths[i]
		}
		if columns[i].ZeroFillWidth > 0 {
			columns[i].Type = fieldTypeVarString
		}
	}
	return columns, streamable, nil
}

func (rs *sqliteResultSet) materialize() (resultSet, error) {
	defer rs.Close()

	resultRows := make([][]any, 0)
	for {
		values, ok, err := rs.nextRow(true)
		if err != nil {
			return resultSet{}, err
		}
		if !ok {
			break
		}
		resultRows = append(resultRows, values)
	}
	return resultSet{Columns: rs.Columns, Rows: resultRows}, nil
}

func (rs *sqliteResultSet) nextRow(refineColumnTypes bool) ([]any, bool, error) {
	if rs.rows == nil {
		return nil, false, nil
	}
	if !rs.rows.Next() {
		return nil, false, rs.rows.Err()
	}
	values := make([]any, len(rs.Columns))
	scan := rs.scanDestinations(values)
	if err := rs.rows.Scan(scan...); err != nil {
		return nil, false, err
	}
	for i, value := range values {
		if rs.Columns[i].ZeroFillWidth > 0 {
			if value != nil {
				values[i] = textValueForColumn(value, rs.Columns[i])
			}
			continue
		}
		if refineColumnTypes && rs.Columns[i].Type == fieldTypeVarString {
			rs.Columns[i].Type = fieldTypeFromValue(value)
		}
	}
	return values, true, nil
}

func (rs *sqliteResultSet) scanDestinations(values []any) []any {
	if cap(rs.scan) < len(values) {
		rs.scan = make([]any, len(values))
	}
	scan := rs.scan[:len(values)]
	for i := range values {
		scan[i] = &values[i]
	}
	return scan
}

func (rs *sqliteResultSet) Close() error {
	if rs.rows == nil {
		return nil
	}
	err := rs.rows.Close()
	rs.rows = nil
	return err
}
