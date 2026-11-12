package snapshot

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadPEMFile(t *testing.T) {
	dir := t.TempDir()
	ok := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ok, []byte("-----BEGIN CERTIFICATE-----\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	big := filepath.Join(dir, "big.pem")
	if err := os.WriteFile(big, make([]byte, maxPEMFileSize+1), 0o600); err != nil {
		t.Fatal(err)
	}

	if data, err := readPEMFile("--ca-cert", filepath.Join(dir, ".", "ca.pem")); err != nil || len(data) == 0 {
		t.Fatalf("regular file: %v", err)
	}
	for _, tc := range []struct{ path, want string }{
		{dir, "not a regular file"},
		{big, "larger than"},
		{filepath.Join(dir, "missing.pem"), "--ca-cert"},
	} {
		if _, err := readPEMFile("--ca-cert", tc.path); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.path, err, tc.want)
		}
	}
}

// TestBuildTLSRejectsNonPEM keeps the old contract: a CA file with no
// certificate in it is an error naming the flag.
func TestBuildTLSRejectsNonPEM(t *testing.T) {
	p := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(p, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := buildTLS(&TLSConfig{CACert: p}); err == nil || !strings.Contains(err.Error(), "--ca-cert") {
		t.Fatalf("err = %v", err)
	}
}
