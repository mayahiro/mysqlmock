package mysqlmock

import (
	"fmt"
	"strings"
)

type informationSchemaTableCacheEntry struct {
	version uint64
	exists  bool
}

type informationSchemaResultSetCacheEntry struct {
	version uint64
	result  resultSet
}

type informationSchemaCache struct {
	fullLoaded     bool
	fullVersion    uint64
	tables         map[string]informationSchemaTableCacheEntry
	showFullFields map[string]informationSchemaResultSetCacheEntry
	showFullOrder  []string
	queryResults   map[string]informationSchemaResultSetCacheEntry
	queryOrder     []string
}

func (cache *informationSchemaCache) hasFullRefresh(version uint64) bool {
	return cache.fullLoaded && cache.fullVersion == version
}

func (cache *informationSchemaCache) markFullRefresh(version uint64) {
	cache.fullLoaded = true
	cache.fullVersion = version
	cache.tables = map[string]informationSchemaTableCacheEntry{}
}

func (cache *informationSchemaCache) tableExists(tableName string, version uint64) (bool, bool) {
	if cache.tables == nil {
		return false, false
	}
	entry, ok := cache.tables[canonicalInformationSchemaTableCacheKey(tableName)]
	if !ok || entry.version != version {
		return false, false
	}
	return entry.exists, true
}

func (cache *informationSchemaCache) markTable(tableName string, version uint64, exists bool) {
	if cache.tables == nil {
		cache.tables = map[string]informationSchemaTableCacheEntry{}
	}
	cache.tables[canonicalInformationSchemaTableCacheKey(tableName)] = informationSchemaTableCacheEntry{
		version: version,
		exists:  exists,
	}
}

func (cache *informationSchemaCache) showFullFieldsResult(currentDB, tableName, likePattern, collation string, version uint64) (resultSet, bool) {
	if cache.showFullFields == nil {
		return resultSet{}, false
	}
	entry, ok := cache.showFullFields[showFullFieldsCacheKey(currentDB, tableName, likePattern, collation)]
	if !ok || entry.version != version {
		return resultSet{}, false
	}
	return cloneResultSet(entry.result), true
}

func (cache *informationSchemaCache) markShowFullFieldsResult(currentDB, tableName, likePattern, collation string, version uint64, result resultSet) {
	key := showFullFieldsCacheKey(currentDB, tableName, likePattern, collation)
	cacheStore(&cache.showFullFields, &cache.showFullOrder, key, informationSchemaResultSetCacheEntry{
		version: version,
		result:  cloneResultSet(result),
	})
}

func (cache *informationSchemaCache) queryResult(key string, version uint64) (resultSet, bool) {
	if cache.queryResults == nil {
		return resultSet{}, false
	}
	entry, ok := cache.queryResults[key]
	if !ok || entry.version != version {
		return resultSet{}, false
	}
	return cloneResultSet(entry.result), true
}

func (cache *informationSchemaCache) markQueryResult(key string, version uint64, result resultSet) {
	cacheStore(&cache.queryResults, &cache.queryOrder, key, informationSchemaResultSetCacheEntry{
		version: version,
		result:  cloneResultSet(result),
	})
}

func (s *Server) cachedShowFullFieldsResult(currentDB, tableName, likePattern, collation string, version uint64) (resultSet, bool) {
	s.informationSchemaMu.Lock()
	defer s.informationSchemaMu.Unlock()
	return s.informationSchema.showFullFieldsResult(currentDB, tableName, likePattern, collation, version)
}

func (s *Server) markShowFullFieldsResult(currentDB, tableName, likePattern, collation string, version uint64, result resultSet) {
	s.informationSchemaMu.Lock()
	defer s.informationSchemaMu.Unlock()
	s.informationSchema.markShowFullFieldsResult(currentDB, tableName, likePattern, collation, version, result)
}

func (s *Server) cachedInformationSchemaQueryResult(key string, version uint64) (resultSet, bool) {
	s.informationSchemaMu.Lock()
	defer s.informationSchemaMu.Unlock()
	return s.informationSchema.queryResult(key, version)
}

func (s *Server) markInformationSchemaQueryResult(key string, version uint64, result resultSet) {
	s.informationSchemaMu.Lock()
	defer s.informationSchemaMu.Unlock()
	s.informationSchema.markQueryResult(key, version, result)
}

func canonicalInformationSchemaTableCacheKey(tableName string) string {
	return strings.ToLower(tableName)
}

func showFullFieldsCacheKey(currentDB, tableName, likePattern, collation string) string {
	return strings.ToLower(currentDB) + "\x00" +
		canonicalInformationSchemaTableCacheKey(tableName) + "\x00" +
		likePattern + "\x00" +
		strings.ToLower(collation)
}

func informationSchemaQueryCacheKey(currentDB, query string, args []any) string {
	var key strings.Builder
	writeInformationSchemaCacheKeyPart(&key, strings.ToLower(currentDB))
	writeInformationSchemaCacheKeyPart(&key, query)
	for _, arg := range args {
		writeInformationSchemaCacheKeyPart(&key, fmt.Sprintf("%T=%#v", arg, arg))
	}
	return key.String()
}

func writeInformationSchemaCacheKeyPart(key *strings.Builder, value string) {
	key.WriteString(fmt.Sprintf("%d:", len(value)))
	key.WriteString(value)
	key.WriteByte('\x00')
}

func cloneResultSet(rs resultSet) resultSet {
	columns := make([]resultColumn, len(rs.Columns))
	copy(columns, rs.Columns)
	rows := make([][]any, len(rs.Rows))
	for i, row := range rs.Rows {
		rows[i] = append([]any(nil), row...)
	}
	return resultSet{Columns: columns, Rows: rows}
}
