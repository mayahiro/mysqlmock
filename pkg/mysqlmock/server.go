package mysqlmock

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// Option configures a Server.
type Option func(*serverOptions)

type serverOptions struct {
	configFile string
	config     *Config
	listen     string
	logWriter  io.Writer
	logFormat  string
}

type preparedSchemaStatement struct {
	Original   string
	Translated []string
}

// ConfigFile loads server configuration from a YAML file.
func ConfigFile(path string) Option {
	return func(opts *serverOptions) {
		opts.configFile = path
	}
}

// WithConfig uses an in-memory configuration value.
func WithConfig(cfg Config) Option {
	return func(opts *serverOptions) {
		opts.config = &cfg
	}
}

// Listen overrides server.listen.
func Listen(addr string) Option {
	return func(opts *serverOptions) {
		opts.listen = addr
	}
}

// LogWriter enables diagnostic logging.
func LogWriter(w io.Writer) Option {
	return func(opts *serverOptions) {
		opts.logWriter = w
	}
}

// LogFormat configures diagnostic logging format. Supported values are text and json.
func LogFormat(format string) Option {
	return func(opts *serverOptions) {
		opts.logFormat = format
	}
}

// Server is a lightweight MySQL-protocol test server backed by SQLite.
type Server struct {
	cfg Config

	rules []preparedRule

	listener net.Listener
	addr     string

	db       *sql.DB
	keepConn *sql.Conn

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	nextConnectionID  atomic.Uint32
	schemaVersion     atomic.Uint64
	baseSchemaVersion atomic.Uint64
	logWriter         io.Writer
	logFormat         string
	logMu             sync.Mutex
	translationMu     sync.Mutex
	preparedMu        sync.Mutex
	metadataMu        sync.Mutex
	diagnostics       diagnosticsStore
	stats             statsStore
	advisoryLocks     advisoryLockStore

	mu                   sync.Mutex
	ruleOnceUsed         map[int]bool
	indexMetadata        map[string]mysqlIndexMetadata
	columnMetadata       map[string]mysqlColumnMetadata
	autoIncrementColumns map[string]mysqlColumnMetadata
	autoIncrement        map[string]uint64
	tableDDL             map[string]string
	translation          sqlTranslationCache

	preparedSchema            []preparedSchemaStatement
	preparedSeed              []map[string][]map[string]any
	tableColumns              map[string]cachedTableColumns
	uniqueKeys                map[string]cachedUniqueKeys
	sqliteAutoIncrementTables map[string]cachedSQLiteAutoIncrementTable
}

// UnsupportedQuery records a query that mysqlmock could not execute.
type UnsupportedQuery struct {
	SQL           string
	NormalizedSQL string
	ConnectionID  uint32
	Command       string
	CurrentDB     string
	RouteStage    string
	Suggestion    string
}

// QueryEvent records one query routed by the server.
type QueryEvent struct {
	Event         string `json:"event"`
	ConnectionID  uint32 `json:"connection_id"`
	Command       string `json:"command"`
	Route         string `json:"route"`
	Database      string `json:"database,omitempty"`
	SQL           string `json:"sql"`
	NormalizedSQL string `json:"normalized_sql,omitempty"`
	Suggestion    string `json:"suggestion,omitempty"`
}

// New creates a server. Call Start before using Addr or DSN.
func New(opts ...Option) (*Server, error) {
	options := serverOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	var cfg Config
	switch {
	case options.config != nil:
		cfg = *options.config
		cfg.applyDefaults()
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
	case options.configFile != "":
		loaded, err := LoadConfigFile(options.configFile)
		if err != nil {
			return nil, err
		}
		cfg = loaded
	default:
		cfg = DefaultConfig()
	}
	if options.listen != "" {
		cfg.Server.Listen = options.listen
	}
	if cfg.Server.ConnectionIDStart == 0 {
		cfg.Server.ConnectionIDStart = 1
	}
	logFormat := options.logFormat
	if logFormat == "" {
		logFormat = "text"
	}
	if logFormat != "text" && logFormat != "json" {
		return nil, fmt.Errorf("unsupported log format: %s", logFormat)
	}
	preparedRules, err := prepareRules(cfg.Rules)
	if err != nil {
		return nil, err
	}

	s := &Server{
		cfg:                       cfg,
		rules:                     preparedRules,
		done:                      make(chan struct{}),
		logWriter:                 options.logWriter,
		logFormat:                 logFormat,
		indexMetadata:             map[string]mysqlIndexMetadata{},
		columnMetadata:            map[string]mysqlColumnMetadata{},
		autoIncrementColumns:      map[string]mysqlColumnMetadata{},
		autoIncrement:             map[string]uint64{},
		tableDDL:                  map[string]string{},
		tableColumns:              map[string]cachedTableColumns{},
		uniqueKeys:                map[string]cachedUniqueKeys{},
		sqliteAutoIncrementTables: map[string]cachedSQLiteAutoIncrementTable{},
	}
	s.nextConnectionID.Store(cfg.Server.ConnectionIDStart)
	return s, nil
}

// Start starts a server for tests and registers cleanup with t.Cleanup.
func Start(t testing.TB, opts ...Option) *Server {
	t.Helper()

	s, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close mysqlmock: %v", err)
		}
	})
	return s
}

// Start begins listening for MySQL client connections.
func (s *Server) Start(ctx context.Context) error {
	if s.listener != nil {
		return errors.New("mysqlmock server already started")
	}
	if err := s.openBackend(ctx); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", s.cfg.Server.Listen)
	if err != nil {
		_ = s.closeBackend()
		return err
	}
	s.listener = ln
	s.addr = ln.Addr().String()

	s.wg.Add(1)
	go s.acceptLoop()
	return nil
}

// Close shuts down the listener and all backend resources.
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.wg.Wait()
		if backendErr := s.closeBackend(); err == nil {
			err = backendErr
		}
	})
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

// Addr returns the listener address.
func (s *Server) Addr() string {
	return s.addr
}

// DSN returns a go-sql-driver/mysql DSN for MVP-0 usage.
func (s *Server) DSN() string {
	dbname := "mysqlmock"
	return fmt.Sprintf("user:password@tcp(%s)/%s?interpolateParams=true&charset=utf8mb4&parseTime=true", s.addr, url.PathEscape(dbname))
}

// Unsupported returns a snapshot of unsupported queries observed by the server.
func (s *Server) Unsupported() []UnsupportedQuery {
	return s.diagnostics.unsupportedSnapshot()
}

// Queries returns a snapshot of query events observed by the server.
func (s *Server) Queries() []QueryEvent {
	return s.diagnostics.queriesSnapshot()
}

// Stats returns a SQL-body-free snapshot of execution counters.
func (s *Server) Stats() Stats {
	return s.stats.snapshot()
}

func (s *Server) currentSchemaVersion() uint64 {
	return s.schemaVersion.Load()
}

func (s *Server) bumpSchemaVersion() {
	s.schemaVersion.Add(1)
	s.stats.recordSchemaChange()
	s.metadataMu.Lock()
	s.tableColumns = map[string]cachedTableColumns{}
	s.uniqueKeys = map[string]cachedUniqueKeys{}
	s.sqliteAutoIncrementTables = map[string]cachedSQLiteAutoIncrementTable{}
	s.metadataMu.Unlock()
}

// Reset restores the configured schema and seed data and clears diagnostics.
// It uses a data-only reset when the schema has not changed, and falls back to
// rebuilding SQLite objects after schema-changing statements.
func (s *Server) Reset(ctx context.Context) error {
	if s.keepConn == nil {
		return errors.New("mysqlmock server not started")
	}
	resetStart := time.Now()
	resetKind := "data_only"
	canResetDataOnly, err := s.canResetBackendData(ctx, s.keepConn)
	if err != nil {
		return err
	}
	if s.currentSchemaVersion() == s.baseSchemaVersion.Load() && canResetDataOnly {
		if err := s.resetBackendData(ctx, s.keepConn); err != nil {
			return err
		}
	} else {
		resetKind = "full"
		s.mu.Lock()
		s.indexMetadata = map[string]mysqlIndexMetadata{}
		s.columnMetadata = map[string]mysqlColumnMetadata{}
		s.autoIncrementColumns = map[string]mysqlColumnMetadata{}
		s.tableDDL = map[string]string{}
		s.autoIncrement = map[string]uint64{}
		s.mu.Unlock()
		if err := s.resetBackendFull(ctx, s.keepConn); err != nil {
			return err
		}
		s.bumpSchemaVersion()
		s.baseSchemaVersion.Store(s.currentSchemaVersion())
	}

	s.diagnostics.reset()
	s.mu.Lock()
	s.ruleOnceUsed = nil
	s.autoIncrement = map[string]uint64{}
	s.mu.Unlock()
	s.stats.recordReset(resetKind)
	s.stats.recordPhaseTiming("reset."+resetKind, time.Since(resetStart))
	return nil
}

// CheckConfigFile validates config and verifies that schema and seed data apply to SQLite.
func CheckConfigFile(ctx context.Context, path string) error {
	cfg, err := LoadConfigFile(path)
	if err != nil {
		return err
	}
	s := &Server{cfg: cfg}
	if err := s.openBackend(ctx); err != nil {
		return err
	}
	return s.closeBackend()
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.done:
				return
			default:
				s.logf("accept error: %v", err)
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleNetConn(conn)
		}()
	}
}

func (s *Server) handleNetConn(conn net.Conn) {
	defer conn.Close()

	ctx := context.Background()
	sqliteConn, err := s.db.Conn(ctx)
	if err != nil {
		s.logf("sqlite connection error: %v", err)
		return
	}
	defer sqliteConn.Close()
	if _, err := sqliteConn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		s.logf("sqlite connection initialization error: %v", err)
		return
	}
	if s.usesPrivateMemoryDB() {
		if err := s.resetBackendFull(ctx, sqliteConn); err != nil {
			s.logf("sqlite connection initialization error: %v", err)
			return
		}
	}

	c := &mysqlConn{
		netConn:                conn,
		sqliteConn:             sqliteConn,
		server:                 s,
		connectionID:           s.nextConnectionID.Add(1) - 1,
		statusFlags:            serverStatusAutocommit,
		currentDB:              "mysqlmock",
		characterSetClient:     s.cfg.Compat.Variables["character_set_client"],
		characterSetConnection: s.cfg.Compat.Variables["character_set_connection"],
		characterSetResults:    s.cfg.Compat.Variables["character_set_results"],
		collationConnection:    s.cfg.Compat.Variables["collation_connection"],
		sessionVariables:       map[string]string{},
		nextStatementID:        1,
		statements:             map[uint32]*preparedStatement{},
	}
	defer s.releaseAdvisoryLocks(c.connectionID)
	if err := c.serve(ctx); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, errRuleDisconnect) {
		s.logf("connection=%d error=%v", c.connectionID, err)
	}
}

func (s *Server) openBackend(ctx context.Context) error {
	dsn := s.sqliteDSN()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)

	keepConn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return err
	}
	if _, err := keepConn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		_ = keepConn.Close()
		_ = db.Close()
		return err
	}

	s.db = db
	s.keepConn = keepConn

	if _, err := s.preparedSchemaStatements(); err != nil {
		_ = s.closeBackend()
		return err
	}
	if _, err := s.preparedSeedBlocks(); err != nil {
		_ = s.closeBackend()
		return err
	}

	if err := withSQLiteTransaction(ctx, keepConn, "initial setup", func() error {
		if err := s.applySchema(ctx, keepConn); err != nil {
			return err
		}
		if err := s.applySeed(ctx, keepConn); err != nil {
			return err
		}
		return nil
	}); err != nil {
		_ = s.closeBackend()
		return err
	}
	s.baseSchemaVersion.Store(s.currentSchemaVersion())
	return nil
}

func (s *Server) closeBackend() error {
	var err error
	if s.keepConn != nil {
		err = s.keepConn.Close()
		s.keepConn = nil
	}
	if s.db != nil {
		if dbErr := s.db.Close(); err == nil {
			err = dbErr
		}
		s.db = nil
	}
	return err
}

func (s *Server) sqliteDSN() string {
	if s.cfg.Backend.Mode == "file" {
		return s.cfg.Backend.Path
	}
	if s.usesPrivateMemoryDB() {
		return ":memory:"
	}
	return fmt.Sprintf("file:mysqlmock_%p?mode=memory&cache=shared", s)
}

func (s *Server) usesPrivateMemoryDB() bool {
	return s.cfg.Backend.Mode == "memory" && !s.cfg.Backend.Shared
}

func (s *Server) applySchema(ctx context.Context, conn *sql.Conn) error {
	statements, err := s.preparedSchemaStatements()
	if err != nil {
		return err
	}
	for _, stmt := range statements {
		if err := s.applyPreparedSchemaStatement(ctx, conn, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) preparedSchemaStatements() ([]preparedSchemaStatement, error) {
	s.preparedMu.Lock()
	defer s.preparedMu.Unlock()
	if s.preparedSchema != nil {
		return s.preparedSchema, nil
	}

	prepared := []preparedSchemaStatement{}
	for _, path := range s.cfg.SchemaFiles {
		statements, err := s.loadSchemaFileStatements(path)
		if err != nil {
			return nil, err
		}
		for _, stmt := range statements {
			if strings.TrimSpace(stmt) == "" {
				continue
			}
			prepared = append(prepared, prepareSchemaStatement(stmt))
		}
	}
	for _, stmt := range s.cfg.Schema {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		prepared = append(prepared, prepareSchemaStatement(stmt))
	}
	s.preparedSchema = prepared
	return s.preparedSchema, nil
}

func prepareSchemaStatement(stmt string) preparedSchemaStatement {
	translated := translateSQLStatements(stmt)
	out := make([]string, 0, len(translated))
	for _, item := range translated {
		if strings.TrimSpace(item) != "" {
			out = append(out, item)
		}
	}
	return preparedSchemaStatement{Original: stmt, Translated: out}
}

func (s *Server) loadSchemaFileStatements(path string) ([]string, error) {
	resolved := path
	if !filepath.IsAbs(resolved) {
		baseDir := s.cfg.schemaBaseDir
		if baseDir == "" {
			baseDir = "."
		}
		resolved = filepath.Join(baseDir, path)
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read schema file %s: %w", path, err)
	}
	return schemaStatementsFromDump(string(data)), nil
}

func (s *Server) applyPreparedSchemaStatement(ctx context.Context, conn *sql.Conn, stmt preparedSchemaStatement) error {
	for _, translated := range stmt.Translated {
		if _, err := conn.ExecContext(ctx, translated); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	s.recordMySQLIndexMetadata(stmt.Original)
	s.recordMySQLColumnMetadata(stmt.Original)
	s.recordMySQLTableDDL(stmt.Original)
	return nil
}

func (s *Server) applySeed(ctx context.Context, conn *sql.Conn) error {
	blocks, err := s.preparedSeedBlocks()
	if err != nil {
		return err
	}
	for _, block := range blocks {
		if err := s.applySeedRows(ctx, conn, block); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) preparedSeedBlocks() ([]map[string][]map[string]any, error) {
	s.preparedMu.Lock()
	defer s.preparedMu.Unlock()
	if s.preparedSeed != nil {
		return s.preparedSeed, nil
	}

	blocks := []map[string][]map[string]any{}
	for _, path := range s.cfg.SeedFiles {
		loaded, err := s.loadSeedFileConfig(SeedFileConfig{Path: path})
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, loaded)
	}
	for _, seedFile := range s.cfg.SeedConfigs {
		loaded, err := s.loadSeedFileConfig(seedFile)
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, loaded)
	}
	if seed := s.cfg.Seed; len(seed) > 0 {
		blocks = append(blocks, seed)
	}
	s.preparedSeed = blocks
	return s.preparedSeed, nil
}

func (s *Server) loadSeedFileConfig(cfg SeedFileConfig) (map[string][]map[string]any, error) {
	path := cfg.Path
	resolved := path
	if !filepath.IsAbs(resolved) {
		baseDir := s.cfg.schemaBaseDir
		if baseDir == "" {
			baseDir = "."
		}
		resolved = filepath.Join(baseDir, path)
	}
	cfg.Path = resolved
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read seed file %s: %w", path, err)
	}
	seed, err := decodeSeedFileConfig(cfg, data)
	if err != nil {
		return nil, fmt.Errorf("read seed file %s: %w", path, err)
	}
	return seed, nil
}

func (s *Server) applySeedRows(ctx context.Context, conn *sql.Conn, seed map[string][]map[string]any) error {
	tables := make([]string, 0, len(seed))
	for table := range seed {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		for _, row := range seed[table] {
			if len(row) == 0 {
				continue
			}
			cols := make([]string, 0, len(row))
			for col := range row {
				cols = append(cols, col)
			}
			sort.Strings(cols)

			placeholders := make([]string, len(cols))
			args := make([]any, len(cols))
			for i, col := range cols {
				placeholders[i] = "?"
				args[i] = row[col]
			}

			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdent(table), joinQuoted(cols), strings.Join(placeholders, ", "))
			if _, err := conn.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf("apply seed for table %s: %w", table, err)
			}
		}
	}
	return nil
}

func (s *Server) resetBackendFull(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("disable foreign keys for reset: %w", err)
	}
	if err := withSQLiteTransaction(ctx, conn, "reset drop", func() error {
		return s.dropSQLiteObjects(ctx, conn)
	}); err != nil {
		_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON")
		return err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys for reset: %w", err)
	}
	return withSQLiteTransaction(ctx, conn, "reset setup", func() error {
		if err := s.applySchema(ctx, conn); err != nil {
			return err
		}
		if err := s.applySeed(ctx, conn); err != nil {
			return err
		}
		return nil
	})
}

func (s *Server) resetBackendData(ctx context.Context, conn *sql.Conn) error {
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return fmt.Errorf("disable foreign keys for reset: %w", err)
	}
	if err := withSQLiteTransaction(ctx, conn, "reset data", func() error {
		if err := s.deleteSQLiteTableRows(ctx, conn); err != nil {
			return err
		}
		return s.resetSQLiteSequences(ctx, conn)
	}); err != nil {
		_, _ = conn.ExecContext(ctx, "PRAGMA foreign_keys = ON")
		return err
	}
	if _, err := conn.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return fmt.Errorf("enable foreign keys for reset: %w", err)
	}
	return withSQLiteTransaction(ctx, conn, "reset seed", func() error {
		return s.applySeed(ctx, conn)
	})
}

func (s *Server) canResetBackendData(ctx context.Context, conn *sql.Conn) (bool, error) {
	var triggerName string
	err := conn.QueryRowContext(ctx, `
SELECT name
FROM sqlite_master
WHERE type = 'trigger'
  AND name NOT LIKE 'sqlite_%'
LIMIT 1`).Scan(&triggerName)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("lookup sqlite triggers for reset: %w", err)
	}
	return false, nil
}

func withSQLiteTransaction(ctx context.Context, conn *sql.Conn, label string, fn func() error) (err error) {
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin %s transaction: %w", label, err)
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
		}
	}()

	if err := fn(); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit %s transaction: %w", label, err)
	}
	committed = true
	return nil
}

type sqliteObject struct {
	Type string
	Name string
}

func (s *Server) dropSQLiteObjects(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `
SELECT type, name
FROM sqlite_master
WHERE type IN ('trigger', 'view', 'table')
  AND name NOT LIKE 'sqlite_%'
ORDER BY CASE type
  WHEN 'trigger' THEN 0
  WHEN 'view' THEN 1
  ELSE 2
END, name`)
	if err != nil {
		return fmt.Errorf("list sqlite objects for reset: %w", err)
	}
	defer rows.Close()

	objects := []sqliteObject{}
	for rows.Next() {
		var obj sqliteObject
		if err := rows.Scan(&obj.Type, &obj.Name); err != nil {
			return fmt.Errorf("scan sqlite object for reset: %w", err)
		}
		objects = append(objects, obj)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite objects for reset: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close sqlite object list for reset: %w", err)
	}

	for _, obj := range objects {
		var stmt string
		switch obj.Type {
		case "trigger":
			stmt = "DROP TRIGGER IF EXISTS " + quoteIdent(obj.Name)
		case "view":
			stmt = "DROP VIEW IF EXISTS " + quoteIdent(obj.Name)
		case "table":
			stmt = "DROP TABLE IF EXISTS " + quoteIdent(obj.Name)
		default:
			continue
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("drop sqlite %s %s for reset: %w", obj.Type, obj.Name, err)
		}
	}
	return nil
}

func (s *Server) deleteSQLiteTableRows(ctx context.Context, conn *sql.Conn) error {
	rows, err := conn.QueryContext(ctx, `
SELECT name
FROM sqlite_master
WHERE type = 'table'
  AND name NOT LIKE 'sqlite_%'
ORDER BY name`)
	if err != nil {
		return fmt.Errorf("list sqlite tables for reset: %w", err)
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return fmt.Errorf("scan sqlite table for reset: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("list sqlite tables for reset: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close sqlite table list for reset: %w", err)
	}

	for _, table := range tables {
		if _, err := conn.ExecContext(ctx, "DELETE FROM "+quoteIdent(table)); err != nil {
			return fmt.Errorf("delete sqlite table %s for reset: %w", table, err)
		}
	}
	return nil
}

func (s *Server) resetSQLiteSequences(ctx context.Context, conn *sql.Conn) error {
	var sequenceTable string
	err := conn.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'sqlite_sequence'`).Scan(&sequenceTable)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lookup sqlite_sequence for reset: %w", err)
	}
	if _, err := conn.ExecContext(ctx, "DELETE FROM sqlite_sequence"); err != nil {
		return fmt.Errorf("reset sqlite_sequence: %w", err)
	}
	return nil
}

func quoteIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func joinQuoted(cols []string) string {
	out := make([]string, len(cols))
	for i, col := range cols {
		out[i] = quoteIdent(col)
	}
	return strings.Join(out, ", ")
}

func (s *Server) recordUnsupported(u UnsupportedQuery) {
	if u.NormalizedSQL == "" {
		u.NormalizedSQL = normalizeSQL(u.SQL)
	}
	if u.RouteStage == "" {
		u.RouteStage = "unsupported"
	}
	if u.Command == "" {
		u.Command = "COM_QUERY"
	}
	u.Suggestion = s.suggestedRule(u)

	s.stats.recordUnsupported()
	s.diagnostics.recordUnsupported(u)
	s.logQuery(QueryEvent{
		Event:         "query",
		ConnectionID:  u.ConnectionID,
		Command:       u.Command,
		Route:         u.RouteStage,
		Database:      u.CurrentDB,
		SQL:           u.SQL,
		NormalizedSQL: u.NormalizedSQL,
		Suggestion:    u.Suggestion,
	})
}

func (s *Server) getAdvisoryLock(name string, connectionID uint32) int {
	return s.advisoryLocks.get(name, connectionID)
}

func (s *Server) releaseAdvisoryLock(name string, connectionID uint32) any {
	return s.advisoryLocks.release(name, connectionID)
}

func (s *Server) releaseAdvisoryLocks(connectionID uint32) {
	s.advisoryLocks.releaseAll(connectionID)
}

func (s *Server) logf(format string, args ...any) {
	if s.logWriter == nil {
		return
	}
	message := fmt.Sprintf(format, args...)
	s.logMu.Lock()
	defer s.logMu.Unlock()

	if s.logFormat == "json" {
		_ = json.NewEncoder(s.logWriter).Encode(map[string]string{
			"event":   "log",
			"message": message,
		})
		return
	}
	fmt.Fprintln(s.logWriter, message)
}

func (s *Server) logQuery(event QueryEvent) {
	if event.Event == "" {
		event.Event = "query"
	}

	s.stats.recordQuery(event.Command, event.Route, event.NormalizedSQL)
	s.diagnostics.recordQuery(event)

	if s.logWriter == nil {
		return
	}

	s.logMu.Lock()
	defer s.logMu.Unlock()

	if s.logFormat == "json" {
		_ = json.NewEncoder(s.logWriter).Encode(event)
		return
	}

	fmt.Fprintf(
		s.logWriter,
		"connection=%d command=%s route=%s database=%q sql=%q",
		event.ConnectionID,
		event.Command,
		event.Route,
		event.Database,
		event.SQL,
	)
	if event.NormalizedSQL != "" && event.NormalizedSQL != event.SQL {
		fmt.Fprintf(s.logWriter, " normalized=%q", event.NormalizedSQL)
	}
	if event.Suggestion != "" {
		fmt.Fprintf(s.logWriter, "\n%s", event.Suggestion)
	}
	fmt.Fprintln(s.logWriter)
}
