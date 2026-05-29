package mysqlmock

import "strings"

type informationSchemaTableCacheEntry struct {
	version uint64
	exists  bool
}

type informationSchemaCache struct {
	fullLoaded  bool
	fullVersion uint64
	tables      map[string]informationSchemaTableCacheEntry
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

func canonicalInformationSchemaTableCacheKey(tableName string) string {
	return strings.ToLower(tableName)
}
