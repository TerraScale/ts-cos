// Package webpush encrypts and sends Web Push messages (RFC 8291, RFC 8188) signed with VAPID (RFC 8292).
package webpush

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	recordSize   = 4096
	maxPlaintext = 3800
	b64url       = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)

var (
	// ErrGone means the subscription is dead and should be deleted.
	ErrGone = errors.New("webpush: subscription expired")

	errKeys       = errors.New("webpush: invalid subscription keys")
	errRecord     = errors.New("webpush: invalid message body")
	errAuth       = errors.New("webpush: invalid authorization")
	errAudience   = errors.New("webpush: authorization audience mismatch")
	errExpired    = errors.New("webpush: authorization expired or lifetime over 24h")
	urgencyValues = []string{"very-low", "low", "normal", "high"}
)

// GenerateVAPID returns a new P-256 private scalar (32 bytes).
func GenerateVAPID() ([]byte, error) {
	k, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return k.Bytes(), nil
}

// VAPID signs push requests for one application server.
type VAPID struct {
	key     *ecdsa.PrivateKey
	subject string
	public  string
}

// ParseVAPID loads a private scalar from GenerateVAPID. subject is a mailto: or https: contact URL.
func ParseVAPID(private []byte, subject string) (*VAPID, error) {
	if subject == "" {
		return nil, errors.New("webpush: VAPID subject is empty")
	}
	key, err := ecdsa.ParseRawPrivateKey(elliptic.P256(), private)
	if err != nil {
		return nil, errors.New("webpush: invalid VAPID private key")
	}
	pub, err := key.PublicKey.ECDH()
	if err != nil {
		return nil, err
	}
	return &VAPID{key: key, subject: subject, public: base64.RawURLEncoding.EncodeToString(pub.Bytes())}, nil
}

// PublicKey is the applicationServerKey the browser subscribes with.
func (v *VAPID) PublicKey() string { return v.public }

// Subscription matches the browser's PushSubscription.toJSON().
type Subscription struct {
	Endpoint string `json:"endpoint"`
	Keys     struct {
		P256dh string `json:"p256dh"`
		Auth   string `json:"auth"`
	} `json:"keys"`
}

// Validate reports whether the endpoint and keys are usable for Encrypt and Send.
func (s Subscription) Validate() error {
	if _, err := audience(s.Endpoint); err != nil {
		return err
	}
	_, _, err := s.keys()
	return err
}

// keys decodes the browser's P-256 public key and auth secret, padded or not.
func (s Subscription) keys() (*ecdh.PublicKey, []byte, error) {
	raw, err := decodeB64(s.Keys.P256dh)
	if err != nil {
		return nil, nil, errKeys
	}
	ua, err := ecdh.P256().NewPublicKey(raw)
	if err != nil {
		return nil, nil, errKeys
	}
	auth, err := decodeB64(s.Keys.Auth)
	if err != nil || len(auth) != 16 {
		return nil, nil, errKeys
	}
	return ua, auth, nil
}

// audience returns the scheme://host origin of an https push endpoint.
func audience(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || len(endpoint) > 1024 {
		return "", errors.New("webpush: invalid endpoint")
	}
	return u.Scheme + "://" + u.Host, nil
}

// Encrypt encrypts plaintext for s with a random salt and ephemeral key.
func Encrypt(s Subscription, plaintext []byte) ([]byte, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	ephemeral, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return encrypt(s, plaintext, salt, ephemeral)
}

// encrypt takes the salt and ephemeral key as inputs so the RFC 8291 vector can run.
func encrypt(s Subscription, plaintext, salt []byte, ephemeral *ecdh.PrivateKey) ([]byte, error) {
	if len(plaintext) > maxPlaintext {
		return nil, errors.New("webpush: payload exceeds 3800 bytes")
	}
	ua, auth, err := s.keys()
	if err != nil {
		return nil, err
	}
	secret, err := ephemeral.ECDH(ua)
	if err != nil {
		return nil, err
	}
	as := ephemeral.PublicKey().Bytes()
	aead, nonce, err := contentCipher(secret, auth, salt, ua.Bytes(), as)
	if err != nil {
		return nil, err
	}
	// 0x02 marks the last record; a single record needs no further padding.
	ct := aead.Seal(nil, nonce, slices.Concat(plaintext, []byte{0x02}), nil)
	hdr := slices.Concat(salt, binary.BigEndian.AppendUint32(nil, recordSize), []byte{byte(len(as))}, as)
	return append(hdr, ct...), nil
}

// Decrypt reverses Encrypt. private is the subscription's key and auth its auth secret.
func Decrypt(private *ecdh.PrivateKey, auth, body []byte) ([]byte, error) {
	if len(body) < 21 {
		return nil, errRecord
	}
	salt, idlen := body[:16], int(body[20])
	if len(body) < 21+idlen {
		return nil, errRecord
	}
	as, ct := body[21:21+idlen], body[21+idlen:]
	sender, err := ecdh.P256().NewPublicKey(as)
	if err != nil {
		return nil, errRecord
	}
	secret, err := private.ECDH(sender)
	if err != nil {
		return nil, errRecord
	}
	aead, nonce, err := contentCipher(secret, auth, salt, private.PublicKey().Bytes(), as)
	if err != nil {
		return nil, err
	}
	pt, err := aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, errRecord
	}
	i := len(pt) - 1
	for i >= 0 && pt[i] == 0 {
		i--
	}
	if i < 0 || pt[i] != 0x02 {
		return nil, errRecord
	}
	return pt[:i], nil
}

// contentCipher derives the record AEAD and nonce (RFC 8291 §3.3-3.4, RFC 8188 §2).
func contentCipher(secret, auth, salt, ua, as []byte) (cipher.AEAD, []byte, error) {
	info := slices.Concat([]byte("WebPush: info\x00"), ua, as)
	ikm, err := hkdf.Key(sha256.New, secret, auth, string(info), 32)
	if err != nil {
		return nil, nil, err
	}
	cek, err := hkdf.Key(sha256.New, ikm, salt, "Content-Encoding: aes128gcm\x00", 16)
	if err != nil {
		return nil, nil, err
	}
	nonce, err := hkdf.Key(sha256.New, ikm, salt, "Content-Encoding: nonce\x00", 12)
	if err != nil {
		return nil, nil, err
	}
	block, err := aes.NewCipher(cek)
	if err != nil {
		return nil, nil, err
	}
	aead, err := cipher.NewGCM(block)
	return aead, nonce, err
}

// Message holds the push headers a caller can set.
type Message struct {
	TTL     time.Duration // header TTL in whole seconds
	Urgency string        // "", "very-low", "low", "normal", "high"; "" omits the header
	Topic   string        // optional; <= 32 chars of the base64url alphabet
}

func (m Message) validate() error {
	if m.TTL < 0 || (m.Urgency != "" && !slices.Contains(urgencyValues, m.Urgency)) {
		return errors.New("webpush: invalid TTL or urgency")
	}
	if len(m.Topic) > 32 || strings.ContainsFunc(m.Topic, func(r rune) bool { return !strings.ContainsRune(b64url, r) }) {
		return errors.New("webpush: invalid topic")
	}
	return nil
}

// Send encrypts payload for s and posts it to the subscription's push service.
func (v *VAPID) Send(ctx context.Context, client *http.Client, s Subscription, payload []byte, m Message) error {
	aud, err := audience(s.Endpoint)
	if err != nil {
		return err
	}
	if err := s.Validate(); err != nil {
		return err
	}
	if err := m.validate(); err != nil {
		return err
	}
	body, err := Encrypt(s, payload)
	if err != nil {
		return err
	}
	tok, err := v.token(time.Now(), aud)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.Endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Encoding", "aes128gcm")
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("TTL", strconv.FormatInt(int64(m.TTL/time.Second), 10))
	req.Header.Set("Authorization", "vapid t="+tok+", k="+v.public)
	if m.Urgency != "" {
		req.Header.Set("Urgency", m.Urgency)
	}
	if m.Topic != "" {
		req.Header.Set("Topic", m.Topic)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.CopyN(io.Discard, resp.Body, 4<<10)
	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted:
		return nil
	case http.StatusNotFound, http.StatusGone:
		return ErrGone
	}
	return fmt.Errorf("webpush: push service returned %d", resp.StatusCode)
}

type claims struct {
	Aud string `json:"aud"`
	Exp int64  `json:"exp"`
	Sub string `json:"sub"`
}

// token returns an ES256 JWT for aud, valid 12 hours (RFC 8292 §2).
func (v *VAPID) token(now time.Time, aud string) (string, error) {
	head := base64.RawURLEncoding.EncodeToString([]byte(`{"typ":"JWT","alg":"ES256"}`))
	body, err := json.Marshal(claims{Aud: aud, Exp: now.Add(12 * time.Hour).Unix(), Sub: v.subject})
	if err != nil {
		return "", err
	}
	in := head + "." + base64.RawURLEncoding.EncodeToString(body)
	sum := sha256.Sum256([]byte(in))
	r, s, err := ecdsa.Sign(rand.Reader, v.key, sum[:])
	if err != nil {
		return "", err
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])
	return in + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// VerifyAuthorization checks a "vapid t=<jwt>, k=<key>" header and returns k.
func VerifyAuthorization(header, audience string, now time.Time) (string, error) {
	rest, ok := strings.CutPrefix(header, "vapid ")
	if !ok {
		return "", errAuth
	}
	var tok, key string
	for _, part := range strings.Split(rest, ",") {
		part = strings.TrimSpace(part)
		if v, ok := strings.CutPrefix(part, "t="); ok {
			tok = v
		}
		if v, ok := strings.CutPrefix(part, "k="); ok {
			key = v
		}
	}
	parts := strings.Split(tok, ".")
	if len(parts) != 3 || key == "" {
		return "", errAuth
	}
	kb, err := decodeB64(key)
	if err != nil {
		return "", errAuth
	}
	pub, err := ecdsa.ParseUncompressedPublicKey(elliptic.P256(), kb)
	if err != nil {
		return "", errAuth
	}
	sig, err := decodeB64(parts[2])
	if err != nil || len(sig) != 64 {
		return "", errAuth
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if !ecdsa.Verify(pub, sum[:], new(big.Int).SetBytes(sig[:32]), new(big.Int).SetBytes(sig[32:])) {
		return "", errAuth
	}
	payload, err := decodeB64(parts[1])
	if err != nil {
		return "", errAuth
	}
	var c claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return "", errAuth
	}
	if c.Aud != audience {
		return "", errAudience
	}
	exp := time.Unix(c.Exp, 0)
	if !exp.After(now) || exp.After(now.Add(24*time.Hour)) {
		return "", errExpired
	}
	return key, nil
}

// decodeB64 accepts base64url with or without padding.
func decodeB64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}
