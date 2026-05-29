package mysqlmock

const sqlTranslationCacheLimit = 4096

type sqlTranslationCache struct {
	sql        map[string]string
	statements map[string][]string
	upserts    map[string]cachedMySQLUpsertStatementTemplate
	inserts    map[string]cachedMySQLInsertStatementTemplate
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
	if s.translation.sql == nil {
		s.translation.sql = map[string]string{}
	}
	if len(s.translation.sql) >= sqlTranslationCacheLimit {
		s.translation.sql = map[string]string{}
	}
	s.translation.sql[sqlText] = translated
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
	if s.translation.statements == nil {
		s.translation.statements = map[string][]string{}
	}
	if len(s.translation.statements) >= sqlTranslationCacheLimit {
		s.translation.statements = map[string][]string{}
	}
	s.translation.statements[sqlText] = append([]string(nil), translated...)
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
	if s.translation.upserts == nil {
		s.translation.upserts = map[string]cachedMySQLUpsertStatementTemplate{}
	}
	if len(s.translation.upserts) >= sqlTranslationCacheLimit {
		s.translation.upserts = map[string]cachedMySQLUpsertStatementTemplate{}
	}
	s.translation.upserts[sqlText] = cachedMySQLUpsertStatementTemplate{template: template, ok: ok}
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
	if s.translation.inserts == nil {
		s.translation.inserts = map[string]cachedMySQLInsertStatementTemplate{}
	}
	if len(s.translation.inserts) >= sqlTranslationCacheLimit {
		s.translation.inserts = map[string]cachedMySQLInsertStatementTemplate{}
	}
	s.translation.inserts[sqlText] = cachedMySQLInsertStatementTemplate{template: template, ok: ok}
	s.translationMu.Unlock()

	if !ok {
		return mysqlInsertStatement{}, false, nil
	}
	stmt, bindOK := template.bind(args)
	return stmt, bindOK, nil
}
