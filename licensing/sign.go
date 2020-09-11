package license

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/gob"
	"errors"
	"strings"
)

var header = "-----BEGIN LICENSE KEY-----"
var footer = "-----END LICENSE KEY-----"

type license struct {
	Payload   []byte
	Signature []byte
}

// signAndEncode encodes any block of data with a RSA private key
// and returns the resulting signed string
func signAndEncode(privateKey *rsa.PrivateKey, i interface{}) (string, error) {
	l, err := sign(privateKey, i)
	if err != nil {
		return "", err
	}
	str, err := encode(l)
	if err != nil {
		return "", err
	}
	return str, nil
}

// decodeAndVerify decodes a previously signed string with its public key
// and returns the original data block
func decodeAndVerify(publicKey *rsa.PublicKey, str string) (interface{}, error) {
	l, err := decode(str)
	if err != nil {
		return nil, err
	}
	i, err := verify(publicKey, l)
	if err != nil {
		return nil, err
	}
	return i, nil
}

func sign(privateKey *rsa.PrivateKey, i interface{}) (*license, error) {
	bytes, err := gobEncode(i)
	if err != nil {
		return nil, err
	}
	hashed := hash(bytes)
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return nil, err
	}
	l := &license{
		Payload:   bytes,
		Signature: signature,
	}
	return l, nil
}

func verify(publicKey *rsa.PublicKey, l *license) (interface{}, error) {
	hashed := hash(l.Payload)
	if err := rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hashed[:], l.Signature); err != nil {
		return nil, err
	}
	i, err := gobDecode(l.Payload)
	if err != nil {
		return nil, err
	}
	return i, nil
}

func encode(l *license) (string, error) {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(l); err != nil {
		return "", err
	}

	b64 := base64.StdEncoding.EncodeToString(buffer.Bytes())
	result := header + "\n"
	width := 64
	for i := 0; ; i += width {
		if i+width <= len(b64) {
			result += b64[i:i+width] + "\n"
		} else {
			result += b64[i:] + "\n"
			break
		}
	}
	result += footer

	return result, nil
}

func decode(str string) (*license, error) {
	str = strings.TrimSpace(str)
	if !strings.HasPrefix(str, header) || !strings.HasSuffix(str, footer) {
		return nil, errors.New("invalid license key format")
	}
	b64 := strings.Replace(str[len(header):len(str)-len(footer)], "\n", "", -1)
	var l license
	b, err := base64.StdEncoding.DecodeString(b64)
	buffer := bytes.Buffer{}
	buffer.Write(b)
	decoder := gob.NewDecoder(&buffer)
	if err = decoder.Decode(&l); err != nil {
		return nil, err
	}
	return &l, nil
}

func hash(b []byte) [32]byte {
	return sha256.Sum256(b)
}

func gobEncode(i interface{}) ([]byte, error) {
	buffer := bytes.Buffer{}
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(&i); err != nil {
		return buffer.Bytes(), err
	}
	return buffer.Bytes(), nil
}

func gobDecode(b []byte) (interface{}, error) {
	var i interface{}
	buffer := bytes.Buffer{}
	buffer.Write(b)
	decoder := gob.NewDecoder(&buffer)
	if err := decoder.Decode(&i); err != nil {
		return nil, err
	}
	return i, nil
}
