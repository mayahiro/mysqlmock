package mysqlmock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	_ "modernc.org/sqlite"
)

// Option configures a Server.
type Option func(*serverOptions)

type serverOptions struct {
	configFile string
	config     *Config
	listen     string
	logWriter  io.Writer
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

// Server is a lightweight MySQL-protocol test server backed by SQLite.
type Server struct {
	cfg Config

	listener net.Listener
	addr     string

	db       *sql.DB
	keepConn *sql.Conn

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup

	nextConnectionID atomic.Uint32
	logWriter        io.Writer

	mu           sync.Mutex
	unsupported  []UnsupportedQuery
	ruleOnceUsed map[int]bool
}

// UnsupportedQuery records a query that mysqlmock could not execute.
type UnsupportedQuery struct {
	SQL           string
	NormalizedSQL string
	ConnectionID  uint32
	CurrentDB     string
	RouteStage    string
	Suggestion    string
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

	s := &Server{
		cfg:       cfg,
		done:      make(chan struct{}),
		logWriter: options.logWriter,
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
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]UnsupportedQuery, len(s.unsupported))
	copy(out, s.unsupported)
	return out
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
		nextStatementID:        1,
		statements:             map[uint32]*preparedStatement{},
	}
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

	if err := s.applySchema(ctx, keepConn); err != nil {
		_ = s.closeBackend()
		return err
	}
	if err := s.applySeed(ctx, keepConn); err != nil {
		_ = s.closeBackend()
		return err
	}
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
	return fmt.Sprintf("file:mysqlmock_%p?mode=memory&cache=shared", s)
}

func (s *Server) applySchema(ctx context.Context, conn *sql.Conn) error {
	for _, stmt := range s.cfg.Schema {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}
	return nil
}

func (s *Server) applySeed(ctx context.Context, conn *sql.Conn) error {
	tables := make([]string, 0, len(s.cfg.Seed))
	for table := range s.cfg.Seed {
		tables = append(tables, table)
	}
	sort.Strings(tables)

	for _, table := range tables {
		for _, row := range s.cfg.Seed[table] {
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
	u.Suggestion = suggestedRule(u.SQL, s.cfg.Fallback.Unsupported)

	s.mu.Lock()
	s.unsupported = append(s.unsupported, u)
	s.mu.Unlock()
	s.logf(
		"connection=%d database=%q route=%s unsupported sql=%q normalized=%q\n%s",
		u.ConnectionID,
		u.CurrentDB,
		u.RouteStage,
		u.SQL,
		u.NormalizedSQL,
		u.Suggestion,
	)
}

func (s *Server) logf(format string, args ...any) {
	if s.logWriter == nil {
		return
	}
	fmt.Fprintf(s.logWriter, format+"\n", args...)
}
