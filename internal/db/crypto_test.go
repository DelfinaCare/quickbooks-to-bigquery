package db

import (
	"bytes"
	"strings"
	"testing"
)

func TestCipherRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	c, err := NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	const secret = "example-refresh-token-value"
	a, _ := c.Encrypt(secret, "realm-1")
	b, _ := c.Encrypt(secret, "realm-1")
	if a == b {
		t.Error("nonces must make ciphertexts differ")
	}
	if strings.Contains(a, secret) || !strings.HasPrefix(a, "v1:") {
		t.Errorf("ciphertext looks wrong: %s", a)
	}
	if got, err := c.Decrypt(a, "realm-1"); err != nil || got != secret {
		t.Errorf("Decrypt = %q, %v", got, err)
	}
	if _, err := c.Decrypt(a, "realm-2"); err == nil {
		t.Error("ciphertext must be bound to its realm")
	}
	tampered := a[:len(a)-4] + "AAAA"
	if _, err := c.Decrypt(tampered, "realm-1"); err == nil {
		t.Error("tampering must be detected")
	}
	other, _ := NewCipher(bytes.Repeat([]byte{8}, 32))
	if _, err := other.Decrypt(a, "realm-1"); err == nil {
		t.Error("wrong key must fail")
	}
	if _, err := NewCipher([]byte("short")); err == nil {
		t.Error("short keys must be rejected")
	}
	if _, err := c.Decrypt("garbage", "realm-1"); err == nil {
		t.Error("garbage must fail")
	}
}
