package mysqlmock

const sqlTranslationCacheLimit = 4096

type sqlTranslationCache struct {
	sql        map[string]string
	statements map[string][]string
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
