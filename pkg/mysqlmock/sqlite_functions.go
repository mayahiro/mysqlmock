package mysqlmock

import (
	"database/sql/driver"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"time"

	sqlite "modernc.org/sqlite"
)

func init() {
	registerSQLiteMySQLFunctions()
}

func registerSQLiteMySQLFunctions() {
	registerSQLiteMySQLFunction("rand", &sqlite.FunctionImpl{
		NArgs: -1,
		Scalar: func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if len(args) > 1 {
				return nil, fmt.Errorf("RAND expects 0 or 1 arguments")
			}
			if len(args) == 0 {
				return rand.Float64(), nil
			}
			if args[0] == nil {
				return nil, nil
			}
			seed := mysqlCompatInt64(args[0])
			return rand.New(rand.NewSource(seed)).Float64(), nil
		},
	})

	registerSQLiteMySQLFunction("find_in_set", &sqlite.FunctionImpl{
		NArgs:         2,
		Deterministic: true,
		Scalar: func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if args[0] == nil || args[1] == nil {
				return nil, nil
			}
			needle := mysqlCompatString(args[0])
			list := mysqlCompatString(args[1])
			if list == "" {
				return int64(0), nil
			}
			for i, item := range strings.Split(list, ",") {
				if item == needle {
					return int64(i + 1), nil
				}
			}
			return int64(0), nil
		},
	})

	registerSQLiteMySQLFunction("field", &sqlite.FunctionImpl{
		NArgs:         -1,
		Deterministic: true,
		Scalar: func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if len(args) < 2 {
				return nil, fmt.Errorf("FIELD expects at least 2 arguments")
			}
			if args[0] == nil {
				return int64(0), nil
			}
			mode := mysqlFieldComparisonMode(args)
			for i := 1; i < len(args); i++ {
				if args[i] == nil {
					continue
				}
				if mysqlFieldEqual(mode, args[0], args[i]) {
					return int64(i), nil
				}
			}
			return int64(0), nil
		},
	})

	registerSQLiteMySQLFunction("regexp", &sqlite.FunctionImpl{
		NArgs:         2,
		Deterministic: true,
		Scalar: func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			if args[0] == nil || args[1] == nil {
				return nil, nil
			}
			matched, err := regexp.MatchString(mysqlCompatString(args[0]), mysqlCompatString(args[1]))
			if err != nil {
				return nil, err
			}
			if matched {
				return int64(1), nil
			}
			return int64(0), nil
		},
	})
}

func registerSQLiteMySQLFunction(name string, impl *sqlite.FunctionImpl) {
	err := sqlite.RegisterFunction(name, impl)
	if err == nil || strings.Contains(err.Error(), "already registered") {
		return
	}
	panic(fmt.Sprintf("register SQLite MySQL compatibility function %s: %v", name, err))
}

type mysqlFieldCompareMode int

const (
	mysqlFieldCompareString mysqlFieldCompareMode = iota
	mysqlFieldCompareNumber
	mysqlFieldCompareDouble
)

func mysqlFieldComparisonMode(args []driver.Value) mysqlFieldCompareMode {
	allStrings := true
	allNumbers := true
	seen := false
	for _, arg := range args {
		if arg == nil {
			continue
		}
		seen = true
		if !mysqlCompatIsString(arg) {
			allStrings = false
		}
		if !mysqlCompatIsNumber(arg) {
			allNumbers = false
		}
	}
	switch {
	case !seen:
		return mysqlFieldCompareString
	case allStrings:
		return mysqlFieldCompareString
	case allNumbers:
		return mysqlFieldCompareNumber
	default:
		return mysqlFieldCompareDouble
	}
}

func mysqlFieldEqual(mode mysqlFieldCompareMode, left, right driver.Value) bool {
	switch mode {
	case mysqlFieldCompareString:
		return mysqlCompatString(left) == mysqlCompatString(right)
	case mysqlFieldCompareNumber, mysqlFieldCompareDouble:
		leftNumber := mysqlCompatFloat64(left)
		rightNumber := mysqlCompatFloat64(right)
		if math.IsNaN(leftNumber) || math.IsNaN(rightNumber) {
			return false
		}
		return leftNumber == rightNumber
	default:
		return false
	}
}

func mysqlCompatIsString(value driver.Value) bool {
	switch value.(type) {
	case string, []byte:
		return true
	default:
		return false
	}
}

func mysqlCompatIsNumber(value driver.Value) bool {
	switch value.(type) {
	case int64, float64, bool:
		return true
	default:
		return false
	}
}

func mysqlCompatString(value driver.Value) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []byte:
		return string(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999999")
	default:
		return fmt.Sprint(v)
	}
}

func mysqlCompatInt64(value driver.Value) int64 {
	switch v := value.(type) {
	case int64:
		return v
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0
		}
		return int64(v)
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		return mysqlCompatStringToInt64(v)
	case []byte:
		return mysqlCompatStringToInt64(string(v))
	default:
		return mysqlCompatStringToInt64(mysqlCompatString(v))
	}
}

func mysqlCompatFloat64(value driver.Value) float64 {
	switch v := value.(type) {
	case int64:
		return float64(v)
	case float64:
		return v
	case bool:
		if v {
			return 1
		}
		return 0
	case string:
		return mysqlCompatStringToFloat64(v)
	case []byte:
		return mysqlCompatStringToFloat64(string(v))
	default:
		return mysqlCompatStringToFloat64(mysqlCompatString(v))
	}
}

func mysqlCompatStringToInt64(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if parsed, err := strconv.ParseInt(value, 10, 64); err == nil {
		return parsed
	}
	return int64(mysqlCompatStringToFloat64(value))
}

func mysqlCompatStringToFloat64(value string) float64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0
	}
	return parsed
}
