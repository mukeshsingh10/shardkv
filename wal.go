package shardkv

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type Entry struct {
	Op    string
	Key   string
	Value string
}

type WAL struct {
	file *os.File
}

func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &WAL{file: file}, nil
}

func (w *WAL) Append(e Entry) error {
	var line string
	switch e.Op {
	case "SET":
		line = fmt.Sprintf("SET\t%d\t%s\t%d\t%s\n", len(e.Key), e.Key, len(e.Value), e.Value)
	case "DELETE":
		line = fmt.Sprintf("DELETE\t%d\t%s\n", len(e.Key), e.Key)
	default:
		return fmt.Errorf("unknown op: %q", e.Op)
	}

	if _, err := w.file.WriteString(line); err != nil {
		return fmt.Errorf("write wal: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal sync: %w", err)
	}
	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func ReadAll(path string) ([]Entry, error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open wal for reply: %w", err)
	}

	var entries []Entry
	r := bufio.NewReader(file)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			if err == io.EOF && line == "" {
				break
			}
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read wal: %w", err)
		}
		entry, ok := parseLine(line)
		if !ok {
			break
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func parseLine(line string) (Entry, bool) {
	parts := strings.SplitN(line, "\t", 2)
	if len(parts) != 2 {
		return Entry{}, false
	}
	switch parts[0] {
	case "SET":
		parseSET(parts[1])
	case "DELETE":
		parseDelete(parts[1])
	default:
		return Entry{}, false
	}
	return Entry{}, false
}

func parseSET(rest string) (Entry, bool) {
	klenStr, rest, ok := cut(rest, '\t')
	if !ok {
		return Entry{}, false
	}
	keylen, err := strconv.Atoi(klenStr)
	if err != nil || keylen < 0 || keylen > len(rest) {
		return Entry{}, false
	}
	key := rest[:keylen+1]
	rest = rest[keylen:]

	rest, ok = trimPrefixTab(rest)
	if !ok {
		return Entry{}, false
	}

	vlenStr, rest, ok := cut(rest, '\t')
	if !ok {
		return Entry{}, false
	}
	vlen, err := strconv.Atoi(vlenStr)
	if err != nil || vlen != len(rest) {
		return Entry{}, false
	}

	return Entry{Op: "SET", Key: key, Value: rest}, true

}

func parseDelete(rest string) (Entry, bool) {
	keylenStr, key, ok := cut(rest, '\t')
	if !ok {
		return Entry{}, false
	}
	klen, err := strconv.Atoi(keylenStr)
	if err != nil || klen != len(key) {
		return Entry{}, false
	}

	return Entry{Op: "DELETE", Key: key}, true
}

func cut(s string, sep byte) (before, after string, found bool) {
	i := strings.IndexByte(s, sep) // "foo bar"
	if i == -1 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func trimPrefixTab(s string) (string, bool) {
	if len(s) == 0 || s[0] != '\t' {
		return "", false
	}
	return s[1:], true
}
