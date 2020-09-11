package license

import (
	"io/ioutil"
	"testing"
)

func TestPrivateKeyNoPEM(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/invalid_key.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = ReadPrivateKeyContents(privateKeyBytes)
	if err == nil {
		t.Error("Private key file should not be readable")
	}
}

func TestPrivateKeyInvalidPEMType(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/private-key_invalid_block.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = ReadPrivateKeyContents(privateKeyBytes)
	if err == nil {
		t.Error("Private key file should not be readable")
	}
}

func TestPrivateKeyInvalidPEMContent(t *testing.T) {
	t.SkipNow() // Unable to generate an unparsable RSA private key content
}

func TestPublicKeyNoPEM(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/invalid_key.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = ReadPublicKeyContents(privateKeyBytes)
	if err == nil {
		t.Error("Public key file should not be readable")
	}
}

func TestPublicKeyInvalidPEMType(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/public-key_invalid_block.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = ReadPublicKeyContents(privateKeyBytes)
	if err == nil {
		t.Error("Public key file should not be readable")
	}
}

func TestPublicKeyInvalidPEMContent(t *testing.T) {
	t.SkipNow() // Unable to generate an unparsable public key content
}

func TestPrivateKeyValid(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = ReadPrivateKeyContents(privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
}

func TestPublicKeyValid(t *testing.T) {
	publicKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key-pub.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = ReadPublicKeyContents(publicKeyBytes)
	if err != nil {
		t.Error(err)
	}
}
