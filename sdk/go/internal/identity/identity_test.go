// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

package identity

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// The golden vector of the runtime: crates/cairn-server/src/identity.rs, test
// golden_vector_is_stable (master cf_test_golden_vector_master_key, tenant acme, host
// sdk.example:8080, body golden-body, nonce golden-nonce-16b). The nonce claim is ignored: it
// only makes each signed request's JWT distinct. The same bytes are re-verified by the
// TypeScript and Python SDKs.
const (
	goldenKID = "publickeyv1_3CPYZuphHAqFNsix9y91xhW8L6Cn9sTuL2tfTVDhZJdr"
	goldenJWT = "eyJhbGciOiJFZERTQSIsImtpZCI6InB1YmxpY2tleXYxXzNDUFladXBoSEFxRk5zaXg5eTkxeGhXOEw2Q245c1R1TDJ0ZlRWRGhaSmRyIiwidHlwIjoiSldUIn0" +
		".eyJhdWQiOiIvaW52b2tlL1MvaCIsImJzaCI6IlpUUDBpVUVFWDhCT1otdzdycVh0RzFwZHVMSDJWdXFWUGtpQ1o0OXR1ZzAiLCJleHAiOjE3MDAwMDAwNjAsImhvc3QiOiJzZGsuZXhhbXBsZTo4MDgwIiwiaWF0IjoxNzAwMDAwMDAwLCJqdGkiOiJhYjowIiwibmJmIjoxNjk5OTk5OTQwLCJub25jZSI6IloyOXNaR1Z1TFc1dmJtTmxMVEUyWWciLCJzdWIiOiJhY21lIn0" +
		".ye5aaOY8bgpblEegb7oSCNtv_Ue0Lx56LK40CuSZC9XaXOb9IwBKHxiLhzf2y3UQkNyjf2rHDErF4DBul47yAg"
	goldenNowMs = 1_700_000_000_123
)

var goldenNow = time.UnixMilli(goldenNowMs)

func headers(h map[string]string) func(string) string {
	return func(name string) string { return h[name] }
}

func mustVerifier(t *testing.T, kids ...string) *Verifier {
	t.Helper()
	v, err := NewVerifier(kids)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestBase58(t *testing.T) {
	if b, ok := Base58Decode("StV1DL6CwTryKyV"); !ok || string(b) != "hello world" {
		t.Fatalf("%q %v", b, ok)
	}
	if b, ok := Base58Decode("112"); !ok || !bytes.Equal(b, []byte{0, 0, 1}) {
		t.Fatalf("%v %v", b, ok)
	}
	if _, ok := Base58Decode("0OIl"); ok {
		t.Fatal("characters outside the alphabet decoded")
	}
	if b, ok := Base58Decode(strings.TrimPrefix(goldenKID, KIDPrefix)); !ok || len(b) != 32 {
		t.Fatalf("golden kid: %d bytes", len(b))
	}
	for _, in := range [][]byte{nil, {0}, {0, 0, 1}, []byte("hello world"), bytes.Repeat([]byte{0xff}, 32)} {
		out, ok := Base58Decode(Base58Encode(in))
		if !ok || !bytes.Equal(out, in) && !(len(in) == 0 && len(out) == 0) {
			t.Errorf("%x: round trip gave %x", in, out)
		}
	}
	if Base58Encode([]byte("hello world")) != "StV1DL6CwTryKyV" {
		t.Fatal("encode")
	}
}

func TestGoldenVectorVerifies(t *testing.T) {
	c, err := mustVerifier(t, goldenKID).Verify(headers(map[string]string{SchemeHeader: "v1", JWTHeader: goldenJWT}), "/invoke/S/h", goldenNow)
	if err != nil {
		t.Fatal(err)
	}
	if c.Aud != "/invoke/S/h" || c.Sub != "acme" || c.JTI != "ab:0" || c.Iat != 1_700_000_000 ||
		c.Nbf != 1_699_999_940 || c.Exp != 1_700_000_060 || c.KID != goldenKID {
		t.Fatalf("claims %+v", c)
	}
	if c.Host == nil || *c.Host != "sdk.example:8080" || c.BSH == nil || *c.BSH != BodyHash([]byte("golden-body")) {
		t.Fatalf("host %v, bsh %v", c.Host, c.BSH)
	}
	// The whole check with the request it was signed for.
	_, err = Check(mustVerifier(t, goldenKID), headers(map[string]string{SchemeHeader: "v1", JWTHeader: goldenJWT}),
		"/invoke/S/h", JTI(ExpectedJTI([]byte{0xab}, 0)),
		Bound{Host: "SDK.example:8080", Body: []byte("golden-body"), HasBody: true}, goldenNow)
	if err != nil {
		t.Fatal(err)
	}
}

func TestGoldenVectorRejections(t *testing.T) {
	v := mustVerifier(t, goldenKID)
	ok := map[string]string{SchemeHeader: "v1", JWTHeader: goldenJWT}
	sig := goldenJWT[len(goldenJWT)-4:]
	flipped := "AAAA"
	if sig == "AAAA" {
		flipped = "BBBB"
	}
	cases := []struct {
		name string
		h    map[string]string
		path string
		now  time.Time
	}{
		{"another path", ok, "/invoke/S/other", goldenNow},
		{"expired", ok, "/invoke/S/h", goldenNow.Add(61 * time.Second)},
		{"not yet valid", ok, "/invoke/S/h", goldenNow.Add(-61 * time.Second)},
		{"signature changed", map[string]string{SchemeHeader: "v1", JWTHeader: goldenJWT[:len(goldenJWT)-4] + flipped}, "/invoke/S/h", goldenNow},
		{"no scheme", map[string]string{JWTHeader: goldenJWT}, "/invoke/S/h", goldenNow},
		{"wrong scheme", map[string]string{SchemeHeader: "v2", JWTHeader: goldenJWT}, "/invoke/S/h", goldenNow},
		{"no JWT", map[string]string{SchemeHeader: "v1"}, "/invoke/S/h", goldenNow},
	}
	for _, c := range cases {
		_, err := v.Verify(headers(c.h), c.path, c.now)
		var ie *Error
		if !errors.As(err, &ie) {
			t.Errorf("%s: %v, want an identity error", c.name, err)
		}
	}
	// The edges of the window are inside it.
	for _, now := range []time.Time{time.Unix(1_699_999_940, 0), time.Unix(1_700_000_060, 999_000_000)} {
		if _, err := v.Verify(headers(ok), "/invoke/S/h", now); err != nil {
			t.Errorf("at %v: %v", now, err)
		}
	}
	// The key of another tenant.
	other := mustVerifier(t, "publickeyv1_2rAxB2eNFycA6hgt7trqW5UXrbn2yQvaK6TYfsZq3aBV")
	if _, err := other.Verify(headers(ok), "/invoke/S/h", goldenNow); err == nil {
		t.Error("verified with another tenant's key")
	}
	for _, bad := range [][]string{{"not-a-kid"}, {KIDPrefix + "0OIl"}, {KIDPrefix + "StV1DL6CwTryKyV"}, {}} {
		if _, err := NewVerifier(bad); err == nil {
			t.Errorf("NewVerifier(%q) accepted", bad)
		}
	}
}

// signer stands in for the runtime: an Ed25519 key and JWTs signed with it.
type signer struct {
	priv ed25519.PrivateKey
	kid  string
}

func newSigner() signer {
	priv := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, 32))
	return signer{priv: priv, kid: KID(priv.Public().(ed25519.PublicKey))}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (s signer) signRaw(head, claims any) string {
	h, _ := json.Marshal(head)
	c, _ := json.Marshal(claims)
	msg := b64(h) + "." + b64(c)
	return msg + "." + b64(ed25519.Sign(s.priv, []byte(msg)))
}

func (s signer) sign(claims map[string]any) string {
	return s.signRaw(map[string]any{"alg": "EdDSA", "typ": "JWT", "kid": s.kid}, claims)
}

func claimsFor(aud, jti string, now time.Time, extra ...any) map[string]any {
	n := now.Unix()
	c := map[string]any{"aud": aud, "sub": "acme", "iat": n, "nbf": n - 60, "exp": n + 60, "jti": jti}
	for i := 0; i+1 < len(extra); i += 2 {
		c[extra[i].(string)] = extra[i+1]
	}
	return c
}

func signed(jwt string) func(string) string {
	return headers(map[string]string{SchemeHeader: SchemeV1, JWTHeader: jwt})
}

func TestCheckInvoke(t *testing.T) {
	s := newSigner()
	v := mustVerifier(t, s.kid)
	now := time.Unix(1_800_000_000, 0)
	id := bytes.Repeat([]byte{0xa1}, 24)
	jti := ExpectedJTI(id, 0)
	if jti != strings.Repeat("a1", 24)+":0" || ExpectedJTI([]byte{0xab}, 12) != "ab:12" {
		t.Fatalf("ExpectedJTI = %q", jti)
	}
	body := []byte("start+input")
	bound := Bound{Host: "127.0.0.1:9080", Body: body, HasBody: true}
	path := "/invoke/S/echo"

	good := s.sign(claimsFor(path, jti, now, "host", "127.0.0.1:9080", "bsh", BodyHash(body)))
	if c, err := Check(v, signed(good), path, JTI(jti), bound, now); err != nil || c.JTI != jti {
		t.Fatalf("%+v %v", c, err)
	}
	// Old runtimes sign neither host nor bsh: nothing to compare.
	if _, err := Check(v, signed(s.sign(claimsFor(path, jti, now))), path, JTI(jti), Bound{}, now); err != nil {
		t.Fatal(err)
	}
	// Without a verifier, verification is off.
	if c, err := Check(nil, headers(nil), path, JTI(jti), bound, now); c != nil || err != nil {
		t.Fatalf("%+v %v", c, err)
	}

	reject := []struct {
		name  string
		jwt   string
		path  string
		bound Bound
		now   time.Time
		why   string
	}{
		{"another attempt", s.sign(claimsFor(path, ExpectedJTI(id, 1), now)), path, bound, now, "jti does not match the request"},
		{"another invocation", s.sign(claimsFor(path, ExpectedJTI(bytes.Repeat([]byte{0xb2}, 24), 0), now)), path, bound, now, "jti does not match the request"},
		{"discover token on invoke", s.sign(claimsFor(path, DiscoverJTI, now)), path, bound, now, "jti does not match the request"},
		{"wrong aud", s.sign(claimsFor("/invoke/S/other", jti, now)), path, bound, now, "aud /invoke/S/other is not /invoke/S/echo"},
		{"expired", s.sign(claimsFor(path, jti, now.Add(-2*time.Minute))), path, bound, now, "JWT is outside its nbf..exp window"},
		{"other body", s.sign(claimsFor(path, jti, now, "bsh", BodyHash([]byte("other")))), path, bound, now, "body does not match the signed hash"},
		{"other host", s.sign(claimsFor(path, jti, now, "host", "evil")), path, bound, now, "JWT was signed for host evil"},
		{"host claim, request without host", s.sign(claimsFor(path, jti, now, "host", "a.example")), path, Bound{}, now, "JWT was signed for host a.example"},
		{"host not in the trusted hosts", s.sign(claimsFor(path, jti, now, "host", "a.example")), path, Bound{Host: "a.example", Hosts: []string{"b.example"}}, now, "JWT was signed for host a.example"},
		{"host claim not a string", s.sign(claimsFor(path, jti, now, "host", 5)), path, bound, now, "claim host is not a string"},
	}
	for _, c := range reject {
		_, err := Check(v, signed(c.jwt), c.path, JTI(jti), c.bound, c.now)
		if err == nil || err.Error() != c.why {
			t.Errorf("%s: %v, want %q", c.name, err, c.why)
		}
	}
	// The trusted hosts replace the request's own host.
	pinned := s.sign(claimsFor(path, jti, now, "host", "public.example"))
	if _, err := Check(v, signed(pinned), path, JTI(jti), Bound{Host: "10.0.0.1", Hosts: []string{"other", "PUBLIC.example"}}, now); err != nil {
		t.Fatal(err)
	}
	// The body is not compared before it is known.
	if _, err := Check(v, signed(s.sign(claimsFor(path, jti, now, "bsh", "x"))), path, JTI(jti), Bound{}, now); err != nil {
		t.Fatal(err)
	}
}

func TestCheckDiscover(t *testing.T) {
	s := newSigner()
	v := mustVerifier(t, s.kid)
	now := time.Unix(1_800_000_000, 0)
	jwt := s.sign(claimsFor("/discover", DiscoverJTI, now))
	if _, err := Check(v, signed(jwt), "/discover", JTI(DiscoverJTI), Bound{}, now); err != nil {
		t.Fatal(err)
	}
	invokeJWT := s.sign(claimsFor("/discover", "ab:0", now))
	if _, err := Check(v, signed(invokeJWT), "/discover", JTI(DiscoverJTI), Bound{}, now); err == nil {
		t.Fatal("an invoke jti passed discovery")
	}
}

func TestMalformedTokens(t *testing.T) {
	s := newSigner()
	v := mustVerifier(t, s.kid)
	now := time.Unix(1_800_000_000, 0)
	path := "/invoke/S/h"
	good := s.sign(claimsFor(path, "j", now))
	parts := strings.Split(good, ".")
	alphabet := "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	// 64 bytes = 512 bits in 86 characters (516 bits): the last character has 4 spare bits.
	last := strings.IndexByte(alphabet, good[len(good)-1])
	twin := good[:len(good)-1] + string(alphabet[last^0b0011])
	cases := []struct {
		name, jwt, why string
	}{
		{"two parts", parts[0] + "." + parts[1], "JWT does not have three parts"},
		{"four parts", good + ".x", "JWT does not have three parts"},
		{"padding", good + "==", "JWT cannot be decoded"},
		{"spare signature bits", twin, "JWT cannot be decoded"},
		{"whitespace", good[:len(good)-10] + "\n" + good[len(good)-10:], "JWT cannot be decoded"},
		{"standard alphabet", strings.NewReplacer("-", "+", "_", "/").Replace(good), "JWT cannot be decoded"},
		{"header not JSON", b64([]byte("nope")) + "." + parts[1] + "." + parts[2], "JWT cannot be decoded"},
		{"header an array", b64([]byte("[]")) + "." + parts[1] + "." + parts[2], "JWT cannot be decoded"},
		{"claims null", parts[0] + "." + b64([]byte("null")) + "." + parts[2], "JWT cannot be decoded"},
		{"not EdDSA", s.signRaw(map[string]any{"alg": "HS256", "kid": s.kid}, claimsFor(path, "j", now)), "JWT is not EdDSA with a kid"},
		{"no kid", s.signRaw(map[string]any{"alg": "EdDSA"}, claimsFor(path, "j", now)), "JWT is not EdDSA with a kid"},
		{"unknown kid", s.signRaw(map[string]any{"alg": "EdDSA", "kid": KIDPrefix + "x"}, claimsFor(path, "j", now)), "kid " + KIDPrefix + "x is not in the identity keys"},
		{"claims from another token", parts[0] + "." + b64([]byte(`{"aud":"/invoke/S/h"}`)) + "." + parts[2], "invalid signature"},
		{"short signature", parts[0] + "." + parts[1] + "." + b64([]byte("short")), "invalid signature"},
		{"missing sub", s.sign(map[string]any{"aud": path, "iat": now.Unix(), "nbf": now.Unix() - 60, "exp": now.Unix() + 60, "jti": "j"}), "claim sub is missing"},
		{"iat a string", s.sign(claimsFor(path, "j", now, "iat", "1")), "claim iat is missing"},
		{"exp fractional", s.sign(claimsFor(path, "j", now, "exp", 1.5)), "claim exp is missing"},
		{"nbf a bool", s.sign(claimsFor(path, "j", now, "nbf", true)), "claim nbf is missing"},
		{"jti a number", s.sign(claimsFor(path, "j", now, "jti", 3)), "claim jti is missing"},
	}
	for _, c := range cases {
		_, err := v.Verify(signed(c.jwt), path, now)
		if err == nil || err.Error() != c.why {
			t.Errorf("%s: %v, want %q", c.name, err, c.why)
		}
	}
	if _, err := v.Verify(signed(good), path, now); err != nil {
		t.Fatalf("the good token: %v", err)
	}
}

func TestUsedTokens(t *testing.T) {
	u := NewUsedTokens(2)
	at := func(ms int64) time.Time { return time.UnixMilli(ms) }
	if !u.Claim("a", 100, at(0)) {
		t.Fatal("first claim")
	}
	if u.Claim("a", 100, at(0)) {
		t.Fatal("signature already used")
	}
	if !u.Claim("b", 100, at(0)) {
		t.Fatal("second token")
	}
	if u.Claim("c", 100, at(50_000)) {
		t.Fatal("ledger full, no entry expired yet")
	}
	if !u.Claim("c", 200, at(100_001)) {
		t.Fatal("an expired entry is cleaned")
	}
	if NewUsedTokens(0).max != DefaultUsedTokens {
		t.Fatal("default size")
	}
}

func TestBodyHash(t *testing.T) {
	// SHA-256 of the empty string.
	if got := BodyHash(nil); got != "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU" {
		t.Fatal(got)
	}
}
