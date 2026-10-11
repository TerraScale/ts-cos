package webpush

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func unb64(t *testing.T, s string) []byte {
	t.Helper()
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newVAPID(t *testing.T) *VAPID {
	t.Helper()
	priv, err := GenerateVAPID()
	if err != nil {
		t.Fatal(err)
	}
	v, err := ParseVAPID(priv, "https://cos.terrascale.tech")
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// newSubscription returns a subscription for a fresh browser key and its private key and auth secret.
func newSubscription(t *testing.T, endpoint string) (Subscription, *ecdh.PrivateKey, []byte) {
	t.Helper()
	ua, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	auth := make([]byte, 16)
	if _, err := rand.Read(auth); err != nil {
		t.Fatal(err)
	}
	var s Subscription
	s.Endpoint = endpoint
	s.Keys.P256dh = base64.RawURLEncoding.EncodeToString(ua.PublicKey().Bytes())
	s.Keys.Auth = base64.RawURLEncoding.EncodeToString(auth)
	return s, ua, auth
}

// RFC 8291 Appendix A.
func TestRFC8291Vector(t *testing.T) {
	const (
		plaintext = "When I grow up, I want to be a watermelon"
		asPrivate = "yfWPiYE-n46HLnH0KqZOF1fJJU3MYrct3AELtAQ-oRw"
		asPublic  = "BP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A8"
		uaPrivate = "q1dXpw3UpT5VOmu_cf_v6ih07Aems3njxI-JWgLcM94"
		uaPublic  = "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4"
		authSec   = "BTBZMqHH6r4Tts7J_aSIgg"
		salt      = "DGv6ra1nlYgDCS1FRnbzlw"
		message   = "DGv6ra1nlYgDCS1FRnbzlwAAEABBBP4z9KsN6nGRTbVYI_c7VJSPQTBtkgcy27mlmlMoZIIgDll6e3vCYLocInmYWAmS6TlzAC8wEqKK6PBru3jl7A_yl95bQpu6cVPTpK4Mqgkf1CXztLVBSt2Ks3oZwbuwXPXLWyouBWLVWGNWQexSgSxsj_Qulcy4a-fN"
	)
	asKey, err := ecdh.P256().NewPrivateKey(unb64(t, asPrivate))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(asKey.PublicKey().Bytes(), unb64(t, asPublic)) {
		t.Fatal("ephemeral public key does not match vector")
	}
	var s Subscription
	s.Endpoint = "https://push.example.net/send/abc"
	s.Keys.P256dh = uaPublic
	s.Keys.Auth = authSec

	got, err := encrypt(s, []byte(plaintext), unb64(t, salt), asKey)
	if err != nil {
		t.Fatal(err)
	}
	want := unb64(t, message)
	if len(got) != len(want) {
		t.Fatalf("length: got %d, want %d", len(got), len(want))
	}
	if !bytes.Equal(got[:86], want[:86]) {
		t.Errorf("header differs:\n got %x\nwant %x", got[:86], want[:86])
	}
	if !bytes.Equal(got[86:], want[86:]) {
		t.Errorf("ciphertext differs:\n got %x\nwant %x", got[86:], want[86:])
	}

	ua, err := ecdh.P256().NewPrivateKey(unb64(t, uaPrivate))
	if err != nil {
		t.Fatal(err)
	}
	pt, err := Decrypt(ua, unb64(t, authSec), want)
	if err != nil {
		t.Fatal(err)
	}
	if string(pt) != plaintext {
		t.Errorf("Decrypt(vector) = %q, want %q", pt, plaintext)
	}
}

func TestRoundTrip(t *testing.T) {
	s, ua, auth := newSubscription(t, "https://push.example.com/send/1")
	payload := []byte(`{"title":"hi"}`)
	body, err := Encrypt(s, payload)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decrypt(ua, auth, body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round trip = %q, want %q", got, payload)
	}

	if _, err := Encrypt(s, make([]byte, 3801)); err == nil {
		t.Error("Encrypt accepted 3801 bytes")
	}
	body, err = Encrypt(s, make([]byte, 3800))
	if err != nil {
		t.Fatalf("Encrypt rejected 3800 bytes: %v", err)
	}
	if got, err := Decrypt(ua, auth, body); err != nil || len(got) != 3800 {
		t.Errorf("3800-byte round trip: len %d, err %v", len(got), err)
	}
}

func TestValidate(t *testing.T) {
	good, _, _ := newSubscription(t, "https://push.example.com/send/1")
	if err := good.Validate(); err != nil {
		t.Fatalf("valid subscription rejected: %v", err)
	}

	httpEndpoint := good
	httpEndpoint.Endpoint = "http://push.example.com/send/1"
	if err := httpEndpoint.Validate(); err == nil {
		t.Error("accepted http:// endpoint")
	}

	badPoint := good
	badPoint.Keys.P256dh = base64.RawURLEncoding.EncodeToString(append([]byte{0x04}, make([]byte, 64)...))
	if err := badPoint.Validate(); err == nil {
		t.Error("accepted off-curve p256dh")
	}

	shortAuth := good
	shortAuth.Keys.Auth = base64.RawURLEncoding.EncodeToString(make([]byte, 15))
	if err := shortAuth.Validate(); err == nil {
		t.Error("accepted 15-byte auth")
	}

	padded := good
	padded.Keys.Auth += "=="[:(4-len(good.Keys.Auth)%4)%4]
	if err := padded.Validate(); err != nil {
		t.Errorf("rejected padded-or-unpadded auth: %v", err)
	}
}

func TestSend(t *testing.T) {
	v := newVAPID(t)
	var srv *httptest.Server
	var ua *ecdh.PrivateKey
	var auth []byte
	var s Subscription
	srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Encoding") != "aes128gcm" {
			t.Errorf("Content-Encoding = %q", r.Header.Get("Content-Encoding"))
		}
		if r.Header.Get("Content-Type") != "application/octet-stream" {
			t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("TTL") != "60" || r.Header.Get("Urgency") != "high" || r.Header.Get("Topic") != "t1" {
			t.Errorf("TTL/Urgency/Topic = %q/%q/%q", r.Header.Get("TTL"), r.Header.Get("Urgency"), r.Header.Get("Topic"))
		}
		k, err := VerifyAuthorization(r.Header.Get("Authorization"), srv.URL, time.Now())
		if err != nil || k != v.PublicKey() {
			t.Errorf("VerifyAuthorization: k=%q err=%v", k, err)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		got, err := Decrypt(ua, auth, body)
		if err != nil || string(got) != "payload" {
			t.Errorf("Decrypt = %q, %v", got, err)
		}
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	s, ua, auth = newSubscription(t, srv.URL+"/send/1")
	m := Message{TTL: time.Minute, Urgency: "high", Topic: "t1"}
	if err := v.Send(context.Background(), srv.Client(), s, []byte("payload"), m); err != nil {
		t.Fatal(err)
	}
}

func TestSendStatus(t *testing.T) {
	v := newVAPID(t)
	ctx := context.Background()

	gone := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGone)
	}))
	defer gone.Close()
	s, _, _ := newSubscription(t, gone.URL+"/send/1")
	if err := v.Send(ctx, gone.Client(), s, []byte("x"), Message{}); !errors.Is(err, ErrGone) {
		t.Errorf("410 error = %v, want ErrGone", err)
	}

	broken := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer broken.Close()
	s, _, _ = newSubscription(t, broken.URL+"/send/1")
	if err := v.Send(ctx, broken.Client(), s, []byte("x"), Message{}); err == nil || !strings.Contains(err.Error(), "500") {
		t.Errorf("500 error = %v, want one mentioning 500", err)
	}
}

func TestVerifyAuthorization(t *testing.T) {
	v := newVAPID(t)
	now := time.Now()
	aud := "https://push.example.com"

	tok, err := v.token(now, aud)
	if err != nil {
		t.Fatal(err)
	}
	header := "vapid t=" + tok + ", k=" + v.PublicKey()
	if k, err := VerifyAuthorization(header, aud, now); err != nil || k != v.PublicKey() {
		t.Fatalf("valid header: k=%q err=%v", k, err)
	}
	if _, err := VerifyAuthorization(header, "https://other.example.com", now); err == nil {
		t.Error("accepted wrong audience")
	}

	old, err := v.token(now.Add(-13*time.Hour), aud)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyAuthorization("vapid t="+old+", k="+v.PublicKey(), aud, now); err == nil {
		t.Error("accepted expired token")
	}
}
