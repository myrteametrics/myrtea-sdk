package license

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// ReadPrivateKeyContents reads a PEM-encoded X.509 RSA private key and returns a
// rsa.PrivateKey that can be used in GenerateFromPayload to generate a license
// key from a payload.
func ReadPrivateKeyContents(contents []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(contents)
	if block == nil {
		return nil, fmt.Errorf("no PEM encoded key found")
	}

	if block.Type != "RSA PRIVATE KEY" {
		return nil, fmt.Errorf("unknown PEM block type %s", block.Type)
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return privateKey, err
}

// ReadPublicKeyContents reads a PEM-encoded X.509 RSA public key and returns a
// rsa.PublicKey that can be used in VerifyAndExtractPayload to verify a license
// key and extract the included payload.
func ReadPublicKeyContents(contents []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(contents)
	if block == nil {
		return nil, fmt.Errorf("no PEM encoded key found")
	}

	if block.Type != "PUBLIC KEY" {
		return nil, fmt.Errorf("unknown PEM block type %s", block.Type)
	}

	publicKey, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}

	return publicKey.(*rsa.PublicKey), nil
}
