package mysqlmock

const sqlTranslationCacheLimit = 4096

type sqlTranslationCache struct {
	sql         map[string]string
	sqlOrder    []string
	statements  map[string][]string
	stmtOrder   []string
	upserts     map[string]cachedMySQLUpsertStatementTemplate
	upsertOrder []string
	inserts     map[string]cachedMySQLInsertStatementTemplate
	insertOrder []string
}

type cachedMySQLUpsertStatementTemplate struct {
	template mysqlUpsertStatementTemplate
	ok       bool
}

type cachedMySQLInsertStatementTemplate struct {
	template mysqlInsertStatementTemplate
	ok       bool
}

func (s *Server) translateSQLCached(sqlText string) string {
	s.translationMu.Lock()
	if s.translation.sql != nil {
		if translated, ok := s.translation.sql[sqlText]; ok {
			s.translationMu.Unlock()
			return translated
		}
	}
	s.translationMu.Unlock()

	translated := translateSQL(sqlText)

	s.translationMu.Lock()
	cacheStore(&s.translation.sql, &s.translation.sqlOrder, sqlText, translated)
	s.translationMu.Unlock()
	return translated
}

func (s *Server) translateSQLStatementsCached(sqlText string) []string {
	s.translationMu.Lock()
	if s.translation.statements != nil {
		if translated, ok := s.translation.statements[sqlText]; ok {
			out := append([]string(nil), translated...)
			s.translationMu.Unlock()
			return out
		}
	}
	s.translationMu.Unlock()

	translated := translateSQLStatements(sqlText)

	s.translationMu.Lock()
	cacheStore(&s.translation.statements, &s.translation.stmtOrder, sqlText, append([]string(nil), translated...))
	s.translationMu.Unlock()
	return translated
}

func (s *Server) parseMySQLUpsertStatementCached(sqlText string, args []any) (mysqlUpsertStatement, bool, error) {
	s.translationMu.Lock()
	if s.translation.upserts != nil {
		if cached, ok := s.translation.upserts[sqlText]; ok {
			s.translationMu.Unlock()
			if !cached.ok {
				return mysqlUpsertStatement{}, false, nil
			}
			stmt, err := cached.template.bind(args)
			return stmt, true, err
		}
	}
	s.translationMu.Unlock()

	template, ok, err := parseMySQLUpsertStatementTemplate(sqlText)
	if err != nil {
		return mysqlUpsertStatement{}, ok, err
	}

	s.translationMu.Lock()
	cacheStore(&s.translation.upserts, &s.translation.upsertOrder, sqlText, cachedMySQLUpsertStatementTemplate{template: template, ok: ok})
	s.translationMu.Unlock()

	if !ok {
		return mysqlUpsertStatement{}, false, nil
	}
	stmt, err := template.bind(args)
	return stmt, true, err
}

func (s *Server) parseMySQLInsertStatementCached(sqlText string, args []any) (mysqlInsertStatement, bool, error) {
	s.translationMu.Lock()
	if s.translation.inserts != nil {
		if cached, ok := s.translation.inserts[sqlText]; ok {
			s.translationMu.Unlock()
			if !cached.ok {
				return mysqlInsertStatement{}, false, nil
			}
			stmt, ok := cached.template.bind(args)
			return stmt, ok, nil
		}
	}
	s.translationMu.Unlock()

	template, ok, err := parseMySQLInsertStatementTemplate(sqlText)
	if err != nil {
		return mysqlInsertStatement{}, ok, err
	}

	s.translationMu.Lock()
	cacheStore(&s.translation.inserts, &s.translation.insertOrder, sqlText, cachedMySQLInsertStatementTemplate{template: template, ok: ok})
	s.translationMu.Unlock()

	if !ok {
		return mysqlInsertStatement{}, false, nil
	}
	stmt, bindOK := template.bind(args)
	return stmt, bindOK, nil
}

func cacheStore[V any](items *map[string]V, order *[]string, key string, value V) {
	if *items == nil {
		*items = map[string]V{}
	}
	if _, ok := (*items)[key]; ok {
		(*items)[key] = value
		return
	}
	for len(*items) >= sqlTranslationCacheLimit {
		if len(*order) == 0 {
			for evict := range *items {
				delete(*items, evict)
				break
			}
			continue
		}
		evict := (*order)[0]
		*order = (*order)[1:]
		if _, ok := (*items)[evict]; !ok {
			continue
		}
		delete(*items, evict)
	}
	(*items)[key] = value
	*order = append(*order, key)
}
