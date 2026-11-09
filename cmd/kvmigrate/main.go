package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/eqr/ravendb-go-client"
	"github.com/eqr/ravendb-go-client/ravenkv"
)

const sourcePageSize = 500

type config struct {
	mode               string
	writersStopped     bool
	sourceURL          string
	sourceIdentityFile string
	sourcePasswordFile string
	scopes             []scope
	ravenURLs          []string
	ravenDatabase      string
	ravenCertificate   string
	ravenPrivateKey    string
	ravenCAFile        string
}

type scope struct {
	Collection string
	AppName    string
}

type record struct {
	Collection string
	AppName    string
	Key        string
	Value      json.RawMessage
}

type report struct {
	Mode       string                    `json:"mode"`
	Counts     map[string]map[string]int `json:"counts"`
	Written    int                       `json:"written,omitempty"`
	Verified   int                       `json:"verified,omitempty"`
	Missing    []string                  `json:"missing,omitempty"`
	Mismatched []string                  `json:"mismatched,omitempty"`
	Extra      []string                  `json:"extra,omitempty"`
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "kvmigrate: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	cfg, err := parseFlags(args, output)
	if err != nil || cfg == nil {
		return err
	}
	identity, err := readProtected(cfg.sourceIdentityFile)
	if err != nil {
		return err
	}
	password, err := readProtected(cfg.sourcePasswordFile)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()

	source := &pocketBase{baseURL: strings.TrimRight(cfg.sourceURL, "/"), http: &http.Client{Timeout: 30 * time.Second}}
	if err := source.authenticate(ctx, identity, password); err != nil {
		return err
	}
	records, fingerprint, err := source.snapshot(ctx, cfg.scopes)
	if err != nil {
		return err
	}
	if err := source.unchanged(ctx, cfg.scopes, fingerprint); err != nil {
		return err
	}
	if err := checkCollisions(records); err != nil {
		return err
	}

	rep := report{Mode: cfg.mode, Counts: countRecords(records)}
	if cfg.mode == "dry-run" {
		return json.NewEncoder(output).Encode(rep)
	}

	docs, err := openRaven(cfg)
	if err != nil {
		return err
	}
	defer docs.Close()

	if cfg.mode == "import" {
		for _, r := range records {
			if err := ravenkv.NewStore(docs, r.Collection, r.AppName).Set(ctx, r.Key, r.Value); err != nil {
				return fmt.Errorf("write %s/%s/%s: %w", r.Collection, r.AppName, r.Key, err)
			}
			rep.Written++
		}
		if err := source.unchanged(ctx, cfg.scopes, fingerprint); err != nil {
			return fmt.Errorf("after import: %w", err)
		}
		return json.NewEncoder(output).Encode(rep)
	}

	if err := verify(ctx, docs, cfg.scopes, records, &rep); err != nil {
		return err
	}
	if err := json.NewEncoder(output).Encode(rep); err != nil {
		return err
	}
	if len(rep.Missing)+len(rep.Mismatched)+len(rep.Extra) > 0 {
		return errors.New("verify found differences")
	}
	return nil
}

func parseFlags(args []string, output io.Writer) (*config, error) {
	flags := flag.NewFlagSet("kvmigrate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	mode := flags.String("mode", "", "dry-run, import, or verify")
	writersStopped := flags.Bool("confirm-source-writers-stopped", false, "confirm every app writing the KV collections is stopped (required for import)")
	sourceURL := flags.String("source-url", "", "PocketBase base URL")
	sourceIdentity := flags.String("source-identity-file", "", "protected file with the PocketBase app user email (PB_EMAIL)")
	sourcePassword := flags.String("source-password-file", "", "protected file with the PocketBase app user password (PB_PASSWORD)")
	scopesValue := flags.String("scopes", "", "comma-separated collection:app pairs, e.g. kv:guide,library_kv: (empty app after the colon)")
	ravenURLs := flags.String("ravendb-urls", "", "comma-separated RavenDB node URLs")
	ravenDatabase := flags.String("ravendb-database", "", "existing RavenDB database")
	ravenCertificate := flags.String("ravendb-certificate-file", "", "RavenDB client certificate")
	ravenPrivateKey := flags.String("ravendb-private-key-file", "", "RavenDB client private key")
	ravenCAFile := flags.String("ravendb-ca-file", "", "optional trusted CA certificate")
	if err := flags.Parse(args); errors.Is(err, flag.ErrHelp) {
		flags.SetOutput(output)
		flags.PrintDefaults()
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	cfg := &config{
		mode:               *mode,
		writersStopped:     *writersStopped,
		sourceURL:          *sourceURL,
		sourceIdentityFile: *sourceIdentity,
		sourcePasswordFile: *sourcePassword,
		ravenURLs:          split(*ravenURLs),
		ravenDatabase:      *ravenDatabase,
		ravenCertificate:   *ravenCertificate,
		ravenPrivateKey:    *ravenPrivateKey,
		ravenCAFile:        *ravenCAFile,
	}
	if flags.NArg() != 0 {
		return nil, errors.New("unexpected positional arguments")
	}
	scopes, err := parseScopes(*scopesValue)
	if err != nil {
		return nil, err
	}
	cfg.scopes = scopes
	if cfg.mode != "dry-run" && cfg.mode != "import" && cfg.mode != "verify" {
		return nil, errors.New("-mode must be dry-run, import, or verify")
	}
	if cfg.sourceURL == "" || cfg.sourceIdentityFile == "" || cfg.sourcePasswordFile == "" || len(cfg.scopes) == 0 {
		return nil, errors.New("-source-url, -source-identity-file, -source-password-file and -scopes are required")
	}
	if cfg.mode == "dry-run" {
		return cfg, nil
	}
	if cfg.mode == "import" && !cfg.writersStopped {
		return nil, errors.New("import requires -confirm-source-writers-stopped")
	}
	if len(cfg.ravenURLs) == 0 || cfg.ravenDatabase == "" {
		return nil, errors.New("-ravendb-urls and -ravendb-database are required")
	}
	if (cfg.ravenCertificate == "") != (cfg.ravenPrivateKey == "") {
		return nil, errors.New("-ravendb-certificate-file and -ravendb-private-key-file go together")
	}
	return cfg, nil
}

type pocketBase struct {
	baseURL string
	token   string
	http    *http.Client
}

func (p *pocketBase) authenticate(ctx context.Context, identity, password string) error {
	body, err := json.Marshal(map[string]string{"identity": identity, "password": password})
	if err != nil {
		return err
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := p.do(ctx, http.MethodPost, "/api/collections/users/auth-with-password", bytes.NewReader(body), &out); err != nil {
		return fmt.Errorf("pocketbase auth: %w", err)
	}
	if out.Token == "" {
		return errors.New("pocketbase auth: empty token")
	}
	p.token = out.Token
	return nil
}

// snapshot reads all scopes and returns their records with a hash of the raw source data.
func (p *pocketBase) snapshot(ctx context.Context, scopes []scope) ([]record, string, error) {
	var all []record
	hash := sha256.New()
	for _, sc := range scopes {
		records, raw, err := p.records(ctx, sc)
		if err != nil {
			return nil, "", err
		}
		all = append(all, records...)
		fmt.Fprintf(hash, "%q %q\n", sc.Collection, sc.AppName)
		for _, item := range raw {
			hash.Write(item)
			hash.Write([]byte("\n"))
		}
	}
	return all, hex.EncodeToString(hash.Sum(nil)), nil
}

func (p *pocketBase) unchanged(ctx context.Context, scopes []scope, fingerprint string) error {
	_, again, err := p.snapshot(ctx, scopes)
	if err != nil {
		return err
	}
	if again != fingerprint {
		return errors.New("source changed during migration; stop all writers and run again")
	}
	return nil
}

// records reads one app's rows of a collection. An empty app reads the whole
// collection, which must then have no rows for any named app.
func (p *pocketBase) records(ctx context.Context, sc scope) ([]record, []json.RawMessage, error) {
	collection := sc.Collection
	var out []record
	var raw []json.RawMessage
	totalItems := -1
	lastID := ""
	for page := 1; ; page++ {
		params := url.Values{}
		params.Set("page", strconv.Itoa(page))
		params.Set("perPage", strconv.Itoa(sourcePageSize))
		params.Set("sort", "+id")
		params.Set("fields", "id,key,appname,value")
		if sc.AppName != "" {
			params.Set("filter", "appname = "+strconv.Quote(sc.AppName))
		}
		var payload struct {
			Page       int               `json:"page"`
			TotalItems int               `json:"totalItems"`
			Items      []json.RawMessage `json:"items"`
		}
		path := "/api/collections/" + url.PathEscape(collection) + "/records?" + params.Encode()
		if err := p.do(ctx, http.MethodGet, path, nil, &payload); err != nil {
			return nil, nil, fmt.Errorf("read %s page %d: %w", collection, page, err)
		}
		if payload.Page != page {
			return nil, nil, fmt.Errorf("read %s: asked for page %d, got %d", collection, page, payload.Page)
		}
		if totalItems < 0 {
			totalItems = payload.TotalItems
		} else if payload.TotalItems != totalItems {
			return nil, nil, fmt.Errorf("read %s: source changed during paging", collection)
		}
		for _, item := range payload.Items {
			var fields struct {
				ID      string          `json:"id"`
				Key     string          `json:"key"`
				AppName string          `json:"appname"`
				Value   json.RawMessage `json:"value"`
			}
			if err := json.Unmarshal(item, &fields); err != nil {
				return nil, nil, fmt.Errorf("read %s: decode record: %w", collection, err)
			}
			if fields.ID == "" || fields.ID <= lastID {
				return nil, nil, fmt.Errorf("read %s: records are not in stable id order", collection)
			}
			lastID = fields.ID
			key := strings.TrimSpace(fields.Key)
			if key == "" {
				return nil, nil, fmt.Errorf("read %s: record %s has an empty key", collection, fields.ID)
			}
			value := fields.Value
			if len(value) == 0 {
				value = json.RawMessage("null")
			}
			if appName := strings.TrimSpace(fields.AppName); appName != sc.AppName {
				return nil, nil, fmt.Errorf("read %s: record %s belongs to appname %q, not %q; name it in -scopes", collection, fields.ID, appName, sc.AppName)
			}
			out = append(out, record{Collection: collection, AppName: sc.AppName, Key: key, Value: value})
			raw = append(raw, item)
		}
		if len(payload.Items) < sourcePageSize || len(out) >= totalItems {
			break
		}
	}
	if len(out) != totalItems {
		return nil, nil, fmt.Errorf("read %s: got %d records, source reports %d", collection, len(out), totalItems)
	}
	return out, raw, nil
}

func (p *pocketBase) do(ctx context.Context, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	resp, err := p.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return json.Unmarshal(data, out)
}

// checkCollisions fails when two records map to the same case-insensitive RavenDB ID.
// Keys are hex-encoded, so only app names that differ by case can collide.
func checkCollisions(records []record) error {
	seen := map[string]record{}
	var collisions []string
	for _, r := range records {
		if strings.Contains(r.AppName, "/") {
			return fmt.Errorf("collection %q: appname %q contains \"/\"", r.Collection, r.AppName)
		}
		id := ravenkv.NewStore(nil, r.Collection, r.AppName).DocumentID(r.Key)
		folded := strings.ToLower(id)
		if prev, ok := seen[folded]; ok {
			collisions = append(collisions, fmt.Sprintf("%q (collection %q, appname %q, key %q) and %q (collection %q, appname %q, key %q)",
				ravenkv.NewStore(nil, prev.Collection, prev.AppName).DocumentID(prev.Key), prev.Collection, prev.AppName, prev.Key,
				id, r.Collection, r.AppName, r.Key))
			continue
		}
		seen[folded] = r
	}
	if len(collisions) > 0 {
		return fmt.Errorf("document ID collisions:\n  %s", strings.Join(collisions, "\n  "))
	}
	return nil
}

func countRecords(records []record) map[string]map[string]int {
	counts := map[string]map[string]int{}
	for _, r := range records {
		if counts[r.Collection] == nil {
			counts[r.Collection] = map[string]int{}
		}
		counts[r.Collection][r.AppName]++
	}
	return counts
}

func verify(ctx context.Context, docs *ravendb.DocumentStore, scopes []scope, records []record, rep *report) error {
	expected := map[scope]map[string]bool{}
	for _, sc := range scopes {
		expected[sc] = map[string]bool{}
	}
	for _, r := range records {
		sc := scope{Collection: r.Collection, AppName: r.AppName}
		if expected[sc] == nil {
			expected[sc] = map[string]bool{}
		}
		expected[sc][r.Key] = true

		store := ravenkv.NewStore(docs, r.Collection, r.AppName)
		got, err := store.Get(ctx, r.Key)
		if errors.Is(err, ravenkv.ErrNotFound) {
			rep.Missing = append(rep.Missing, store.DocumentID(r.Key))
			continue
		}
		if err != nil {
			return fmt.Errorf("read %s: %w", store.DocumentID(r.Key), err)
		}
		equal, err := sameJSON(r.Value, got)
		if err != nil {
			return fmt.Errorf("compare %s: %w", store.DocumentID(r.Key), err)
		}
		if !equal {
			rep.Mismatched = append(rep.Mismatched, store.DocumentID(r.Key))
			continue
		}
		rep.Verified++
	}
	for sc, keys := range expected {
		store := ravenkv.NewStore(docs, sc.Collection, sc.AppName)
		stored, err := store.List(ctx, "")
		if err != nil {
			return fmt.Errorf("list %s/%s: %w", sc.Collection, sc.AppName, err)
		}
		for _, key := range stored {
			if !keys[key] {
				rep.Extra = append(rep.Extra, store.DocumentID(key))
			}
		}
	}
	sort.Strings(rep.Extra)
	return nil
}

func sameJSON(a, b []byte) (bool, error) {
	var left, right any
	if err := decodeNumbers(a, &left); err != nil {
		return false, err
	}
	if err := decodeNumbers(b, &right); err != nil {
		return false, err
	}
	return reflect.DeepEqual(left, right), nil
}

func decodeNumbers(data []byte, out *any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	return decoder.Decode(out)
}

func openRaven(cfg *config) (*ravendb.DocumentStore, error) {
	docs := ravendb.NewDocumentStore(cfg.ravenURLs, cfg.ravenDatabase)
	if cfg.ravenCertificate != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ravenCertificate, cfg.ravenPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("load RavenDB client certificate: %w", err)
		}
		docs.Certificate = &cert
	}
	if cfg.ravenCAFile != "" {
		caPEM, err := os.ReadFile(cfg.ravenCAFile)
		if err != nil {
			return nil, fmt.Errorf("read RavenDB CA file: %w", err)
		}
		block, _ := pem.Decode(caPEM)
		if block == nil || block.Type != "CERTIFICATE" {
			return nil, errors.New("RavenDB CA file has no PEM certificate")
		}
		root, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse RavenDB CA file: %w", err)
		}
		docs.TrustStore = root
	}
	if err := docs.Initialize(); err != nil {
		return nil, fmt.Errorf("connect to RavenDB: %w", err)
	}
	return docs, nil
}

func readProtected(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("%s: must be a regular file readable only by its owner", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	value := strings.TrimRight(string(data), "\r\n")
	if strings.TrimSpace(value) == "" {
		return "", fmt.Errorf("%s: empty", path)
	}
	return value, nil
}

func parseScopes(value string) ([]scope, error) {
	var scopes []scope
	seen := map[scope]bool{}
	for _, item := range split(value) {
		collection, app, ok := strings.Cut(item, ":")
		collection, app = strings.TrimSpace(collection), strings.TrimSpace(app)
		if !ok || collection == "" || strings.ContainsAny(collection+app, "/\"'") {
			return nil, fmt.Errorf("invalid scope %q: want collection:app", item)
		}
		sc := scope{Collection: collection, AppName: app}
		if seen[sc] {
			return nil, fmt.Errorf("duplicate scope %q", item)
		}
		seen[sc] = true
		scopes = append(scopes, sc)
	}
	return scopes, nil
}

func split(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
