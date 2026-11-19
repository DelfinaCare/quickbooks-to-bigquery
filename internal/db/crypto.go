// Package db stores QBO installations and their encrypted refresh tokens in PostgreSQL.
package db

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// Cipher encrypts refresh tokens with AES-256-GCM.
type Cipher struct{ aead cipher.AEAD }

const cipherPrefix = "v1:"

// NewCipher expects a 32-byte key.
func NewCipher(key []byte) (*Cipher, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead}, nil
}

// Encrypt returns "v1:" + base64(nonce || ciphertext). aad (the realm id)
// binds the ciphertext to its row so it cannot be swapped between companies.
func (c *Cipher) Encrypt(plaintext, aad string) (string, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := c.aead.Seal(nonce, nonce, []byte(plaintext), []byte(aad))
	return cipherPrefix + base64.StdEncoding.EncodeToString(ct), nil
}

// Decrypt reverses Encrypt.
func (c *Cipher) Decrypt(encoded, aad string) (string, error) {
	if !strings.HasPrefix(encoded, cipherPrefix) {
		return "", errors.New("unknown ciphertext format")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(encoded, cipherPrefix))
	if err != nil {
		return "", err
	}
	ns := c.aead.NonceSize()
	if len(raw) < ns {
		return "", errors.New("ciphertext too short")
	}
	pt, err := c.aead.Open(nil, raw[:ns], raw[ns:], []byte(aad))
	if err != nil {
		return "", fmt.Errorf("decrypt: %w (wrong TOKEN_ENCRYPTION_KEY?)", err)
	}
	return string(pt), nil
}
