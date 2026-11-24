# Certificates of the fake TLS edge (`tests/common/tls_edge.rs`)

Test only. `ca.pem` is a self-signed CA (its key is **not** kept); `edge.pem`/`edge.key` is a leaf
for `localhost` and `127.0.0.1`, signed by that CA. P-256 named curve, signature
`ecdsa-with-SHA256` (rustls/webpki reject SHA-1 and explicit curve parameters — the LibreSSL
default on macOS). Valid until 2126.

Regenerate (LibreSSL or OpenSSL):

```sh
openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt -out ca.key
openssl ecparam -name prime256v1 -genkey -noout | openssl pkcs8 -topk8 -nocrypt -out edge.key
openssl req -x509 -new -sha256 -key ca.key -out ca.pem -days 36500 -subj "/CN=cairnflow test CA" \
  -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign"
openssl req -new -key edge.key -subj "/CN=localhost" -out edge.csr
printf "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n" > ext.cnf
openssl x509 -req -sha256 -in edge.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out edge.pem \
  -days 36500 -extfile ext.cnf
rm ca.key ca.srl edge.csr ext.cnf
```
