// Copyright (c) The Cairnflow Authors
// SPDX-License-Identifier: Apache-2.0

// Package identity verifies the runtime's request identity (docs/protocol-v1.md section 4,
// "Request identity").
//
// A runtime with an identity master signs every GET /discover and POST /invoke/... with the
// Ed25519 key of the tenant that owns the deployment, in two HTTP headers:
//
//   - x-cairn-signature-scheme: v1
//   - x-cairn-jwt-v1: an EdDSA JWT. Header {"alg":"EdDSA","typ":"JWT","kid":"publickeyv1_<base58
//     public key>"}; claims aud (the request path, endpoint prefix included), sub (tenant), iat,
//     nbf = iat - 60, exp = iat + 60 (Unix seconds), jti, and on newer runtimes host (the Host
//     the runtime sent to) and, on /invoke, bsh (base64url SHA-256 of the body before
//     compression; in BIDI_STREAM the turn-opening body).
//
// The jti of /invoke is "<hex of Start.id>:<Start.retry_count_since_last_stored_entry>"
// (ExpectedJTI), binding the JWT to this very request's Start; the jti of /discover is
// DiscoverJTI.
//
// An endpoint configured with trusted kids (the ones listed by the runtime's GET /identity)
// answers 401 to a request that does not verify, before running a handler. In BIDI_STREAM the
// jti can only be compared once Start arrives, after the response headers are sent: a mismatch
// closes the stream without sending a frame.
//
// Typical use: NewVerifier once with the trusted kids; per request, Check with the request
// headers, path, expected jti and the parts of the request the JWT binds (Bound). For
// BIDI_STREAM, also UsedTokens.Claim with the JWT's signature so one JWT opens one stream.
package identity

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Header names and fixed values.
const (
	SchemeHeader = "x-cairn-signature-scheme"
	SchemeV1     = "v1"
	JWTHeader    = "x-cairn-jwt-v1"
	// KIDPrefix starts every key id: KIDPrefix + base58 of the 32-byte public key.
	KIDPrefix = "publickeyv1_"
	// DiscoverJTI is the jti of a discovery request: there is no invocation to bind to.
	DiscoverJTI = "discover"
)

// Error is a request whose identity does not verify. Its message says why; it is safe to log
// (it never contains the token).
type Error struct {
	Reason string
}

func (e *Error) Error() string { return e.Reason }

func fail(format string, args ...any) *Error {
	return &Error{Reason: fmt.Sprintf(format, args...)}
}

// Claims are the claims of a verified JWT.
type Claims struct {
	Aud string
	Sub string
	Iat int64
	Nbf int64
	Exp int64
	JTI string
	// KID is the key that signed.
	KID string
	// Host is the host the runtime sent to; nil on runtimes that do not sign it.
	Host *string
	// BSH is base64url(SHA-256) of the body before compression; nil on discovery and on
	// runtimes that do not sign it.
	BSH *string
}

// Verifier holds an endpoint's trusted keys. It is safe for concurrent use.
type Verifier struct {
	keys map[string]ed25519.PublicKey
}

// NewVerifier returns a verifier trusting kids, each KIDPrefix + base58 of a 32-byte Ed25519
// public key. An empty list or a malformed kid is an error.
func NewVerifier(kids []string) (*Verifier, error) {
	keys := make(map[string]ed25519.PublicKey, len(kids))
	for _, kid := range kids {
		var raw []byte
		if rest, ok := strings.CutPrefix(kid, KIDPrefix); ok {
			raw, _ = Base58Decode(rest)
		}
		if len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("identity keys: %q is not %s<base58 of 32 bytes>", kid, KIDPrefix)
		}
		keys[kid] = ed25519.PublicKey(raw)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("identity keys: the list is empty")
	}
	return &Verifier{keys: keys}, nil
}

// Verify checks the identity headers of a request to path at time now: the scheme, the JWT's
// form, the kid, the signature, aud = path and nbf <= now <= exp (whole seconds). header looks
// up a request header by name (http.Header.Get fits). It returns an *Error when the request
// does not verify. The jti, host and bsh claims are not compared here: Check does that.
func (v *Verifier) Verify(header func(name string) string, path string, now time.Time) (*Claims, error) {
	if header(SchemeHeader) != SchemeV1 {
		return nil, fail("missing %s: %s", SchemeHeader, SchemeV1)
	}
	token := header(JWTHeader)
	if token == "" {
		return nil, fail("missing %s", JWTHeader)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fail("JWT does not have three parts")
	}
	headRaw, err1 := b64urlDecode(parts[0])
	claimsRaw, err2 := b64urlDecode(parts[1])
	sig, err3 := b64urlDecode(parts[2])
	if err1 != nil || err2 != nil || err3 != nil {
		return nil, fail("JWT cannot be decoded")
	}
	var head, claims map[string]any
	if decodeObject(headRaw, &head) != nil || decodeObject(claimsRaw, &claims) != nil {
		return nil, fail("JWT cannot be decoded")
	}
	kid, kidOK := head["kid"].(string)
	if alg, _ := head["alg"].(string); alg != "EdDSA" || !kidOK {
		return nil, fail("JWT is not EdDSA with a kid")
	}
	key, ok := v.keys[kid]
	if !ok {
		return nil, fail("kid %s is not in the identity keys", kid)
	}
	if !ed25519.Verify(key, []byte(parts[0]+"."+parts[1]), sig) {
		return nil, fail("invalid signature")
	}
	c := &Claims{KID: kid}
	var err *Error
	text := func(k string) string {
		s, ok := claims[k].(string)
		if !ok && err == nil {
			err = fail("claim %s is missing", k)
		}
		return s
	}
	num := func(k string) int64 {
		n, ok := claims[k].(json.Number)
		var i int64
		var perr error
		if ok {
			i, perr = strconv.ParseInt(string(n), 10, 64)
		}
		if (!ok || perr != nil) && err == nil {
			err = fail("claim %s is missing", k)
		}
		return i
	}
	opt := func(k string) *string {
		raw, present := claims[k]
		if !present || raw == nil {
			return nil
		}
		s, ok := raw.(string)
		if !ok {
			if err == nil {
				err = fail("claim %s is not a string", k)
			}
			return nil
		}
		return &s
	}
	c.Aud, c.Sub = text("aud"), text("sub")
	c.Iat, c.Nbf, c.Exp = num("iat"), num("nbf"), num("exp")
	c.JTI = text("jti")
	c.Host, c.BSH = opt("host"), opt("bsh")
	if err != nil {
		return nil, err
	}
	if c.Aud != path {
		return nil, fail("aud %s is not %s", c.Aud, path)
	}
	// Whole seconds, rounded down, as the runtime and the other SDKs do.
	if sec := now.Unix(); sec < c.Nbf || sec > c.Exp {
		return nil, fail("JWT is outside its nbf..exp window")
	}
	return c, nil
}

// decodeObject parses a JSON object; numbers stay json.Number so integers are exact.
func decodeObject(b []byte, out *map[string]any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if err := d.Decode(out); err != nil {
		return err
	}
	if *out == nil {
		return fmt.Errorf("not an object")
	}
	if d.More() {
		return fmt.Errorf("trailing data")
	}
	return nil
}

// Bound holds the parts of the request a JWT binds to.
type Bound struct {
	// Host is the request's own Host (h2: :authority); empty when the request carried none.
	Host string
	// Hosts are the trusted public hosts; when set they replace Host in the comparison (a proxy
	// between the runtime and the SDK may rewrite Host).
	Hosts []string
	// Body is the request body after decompression, the bytes the runtime hashed before it
	// compressed them (BIDI_STREAM: the turn-opening body); it
	// is compared with the bsh claim only when HasBody is true (headers are checked before the
	// body is read).
	Body    []byte
	HasBody bool
}

// JTI returns a jti matcher for one expected value, for Check.
func JTI(expected string) func(claimed string) bool {
	return func(claimed string) bool { return claimed == expected }
}

// Check verifies a request completely: Verify, then the jti (match), then the host and bsh
// claims when present (old runtimes do not sign them). A host claim is compared,
// case-insensitively, with bound.Hosts when set, otherwise with bound.Host; a request with no
// host and no trusted hosts does not pass. A nil verifier means verification is off: Check
// returns nil, nil.
func Check(v *Verifier, header func(name string) string, path string, match func(claimed string) bool,
	bound Bound, now time.Time) (*Claims, error) {
	if v == nil {
		return nil, nil
	}
	c, err := v.Verify(header, path, now)
	if err != nil {
		return nil, err
	}
	if !match(c.JTI) {
		return nil, fail("jti does not match the request")
	}
	if c.Host != nil {
		trusted := bound.Hosts
		if len(trusted) == 0 && bound.Host != "" {
			trusted = []string{bound.Host}
		}
		ok := false
		for _, h := range trusted {
			if strings.EqualFold(h, *c.Host) {
				ok = true
				break
			}
		}
		if !ok {
			return nil, fail("JWT was signed for host %s", *c.Host)
		}
	}
	if c.BSH != nil && bound.HasBody && BodyHash(bound.Body) != *c.BSH {
		return nil, fail("body does not match the signed hash")
	}
	return c, nil
}

// ExpectedJTI is the jti the runtime sets for the turn whose Start carries invocationID and
// retryCount (Start.retry_count_since_last_stored_entry).
func ExpectedJTI(invocationID []byte, retryCount uint32) string {
	return hex.EncodeToString(invocationID) + ":" + strconv.FormatUint(uint64(retryCount), 10)
}

// BodyHash is the form of the bsh claim: base64url without padding of SHA-256(body).
func BodyHash(body []byte) string {
	sum := sha256.Sum256(body)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// KID returns the key id of an Ed25519 public key.
func KID(pub ed25519.PublicKey) string { return KIDPrefix + Base58Encode(pub) }

// b64urlDecode accepts canonical base64url only: the URL alphabet, no padding, no whitespace,
// zero spare bits. One byte sequence has one spelling, so a JWT string can be used as a ledger
// key (UsedTokens) without variants slipping past it.
func b64urlDecode(s string) ([]byte, error) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return nil, fmt.Errorf("not canonical base64url")
		}
	}
	b, err := base64.RawURLEncoding.Strict().DecodeString(s)
	if err != nil || base64.RawURLEncoding.EncodeToString(b) != s {
		return nil, fmt.Errorf("not canonical base64url")
	}
	return b, nil
}

const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// Base58Decode decodes base58 with the Bitcoin alphabet. A character outside the alphabet
// reports false.
func Base58Decode(s string) ([]byte, bool) {
	zeros := 0
	for zeros < len(s) && s[zeros] == '1' {
		zeros++
	}
	var acc []byte // little-endian
	for i := zeros; i < len(s); i++ {
		carry := strings.IndexByte(alphabet, s[i])
		if carry < 0 {
			return nil, false
		}
		for j := range acc {
			carry += int(acc[j]) * 58
			acc[j] = byte(carry)
			carry >>= 8
		}
		for carry > 0 {
			acc = append(acc, byte(carry))
			carry >>= 8
		}
	}
	out := make([]byte, zeros+len(acc))
	for i, b := range acc {
		out[len(out)-1-i] = b
	}
	return out, true
}

// Base58Encode encodes b with the Bitcoin alphabet.
func Base58Encode(b []byte) string {
	zeros := 0
	for zeros < len(b) && b[zeros] == 0 {
		zeros++
	}
	var digits []byte // little-endian, base 58
	for _, c := range b[zeros:] {
		carry := int(c)
		for j := range digits {
			carry += int(digits[j]) << 8
			digits[j] = byte(carry % 58)
			carry /= 58
		}
		for carry > 0 {
			digits = append(digits, byte(carry%58))
			carry /= 58
		}
	}
	out := make([]byte, zeros+len(digits))
	for i := 0; i < zeros; i++ {
		out[i] = '1'
	}
	for i, d := range digits {
		out[len(out)-1-i] = alphabet[d]
	}
	return string(out)
}

// UsedTokens remembers the JWTs that have opened a BIDI_STREAM stream, until their exp. In
// BIDI_STREAM only the turn-opening body is bound to the JWT (bsh), so someone holding both
// the JWT and that body could open a second stream and send a forged continuation. The
// runtime signs a new JWT for every request (a fresh nonce claim: iat and the turn-opening
// body alone repeat when a turn is resent within one second), so one JWT opening one stream
// is enough. Key it by
// the JWT's signature (its third part), not by jti: a continuation turn reuses the jti.
//
// The ledger is bounded: when full, expired entries are purged; still full, Claim refuses
// (refuse when in doubt). It is safe for concurrent use.
type UsedTokens struct {
	mu   sync.Mutex
	seen map[string]int64
	max  int
}

// DefaultUsedTokens is the default ledger size.
const DefaultUsedTokens = 100_000

// NewUsedTokens returns a ledger holding at most max tokens (DefaultUsedTokens when max <= 0).
func NewUsedTokens(max int) *UsedTokens {
	if max <= 0 {
		max = DefaultUsedTokens
	}
	return &UsedTokens{seen: make(map[string]int64), max: max}
}

// Claim records token, valid until expS (Unix seconds). It reports true the first time a token
// is seen, false when it was already used or the ledger is full.
func (u *UsedTokens) Claim(token string, expS int64, now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if _, used := u.seen[token]; used {
		return false
	}
	if len(u.seen) >= u.max {
		nowMs := now.UnixMilli()
		for k, exp := range u.seen {
			if exp*1000 <= nowMs {
				delete(u.seen, k)
			}
		}
		if len(u.seen) >= u.max {
			return false
		}
	}
	u.seen[token] = expS
	return true
}
