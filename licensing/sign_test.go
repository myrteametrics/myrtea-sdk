package license

import (
	"encoding/gob"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

// TestLicenseValid check if all the library works properly by generating and verifying a new license
func TestLicenseValid(t *testing.T) {
	gob.Register(MyrteaLicenseData{})

	// Server-side license generation
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	privateKey, err := ReadPrivateKeyContents(privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
	ld := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	encodedLicense, err := signAndEncode(privateKey, ld)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("myrtea-license.key", []byte(encodedLicense), 0644)
	if err != nil {
		t.Error(err)
	}

	// Client-side license validation
	publicKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key-pub.pem")
	if err != nil {
		t.Error(err)
	}
	publicKey, err := ReadPublicKeyContents(publicKeyBytes)
	if err != nil {
		t.Error(err)
	}
	readLicense, err := ioutil.ReadFile("myrtea-license.key")
	if err != nil {
		t.Error(err)
	}
	decodedData, err := decodeAndVerify(publicKey, string(readLicense))
	if err != nil {
		t.Error(err)
	}
	decodedLicense := decodedData.(ILicenseData)
	if ok, _ := decodedLicense.Validate(); !ok {
		t.Error(errors.New("License should be valid"))
	}

	// Cleaning
	err = os.Remove("myrtea-license.key")
	if err != nil {
		t.Error(err)
	}
}

// TestLicenseExpired check if an expired license is properly rejected during the verification phase
func TestLicenseValidButExpired(t *testing.T) {
	gob.Register(MyrteaLicenseData{})

	// Server-side license generation
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	privateKey, err := ReadPrivateKeyContents(privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
	ld := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 1*time.Millisecond, "Myrtea Issuer")
	encodedLicense, err := signAndEncode(privateKey, ld)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("myrtea-license.key", []byte(encodedLicense), 0644)
	if err != nil {
		t.Error(err)
	}

	time.Sleep(10 * time.Millisecond)

	// Client-side license validation
	publicKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key-pub.pem")
	if err != nil {
		t.Error(err)
	}
	publicKey, err := ReadPublicKeyContents(publicKeyBytes)
	if err != nil {
		t.Error(err)
	}
	readLicense, err := ioutil.ReadFile("myrtea-license.key")
	if err != nil {
		t.Error(err)
	}
	decodedData, err := decodeAndVerify(publicKey, string(readLicense))
	if err != nil {
		t.Error(err)
	}
	decodedLicense := decodedData.(ILicenseData)

	if ok, _ := decodedLicense.Validate(); ok {
		t.Error(errors.New("License is expired and should not be valid"))
	}

	err = os.Remove("myrtea-license.key")
	if err != nil {
		t.Error(err)
	}
}

// TestLicenseFalsified checks if a falsified license is properly rejected during the verification phase
func TestLicenseInvalidFalsified(t *testing.T) {
	gob.Register(MyrteaLicenseData{})
	// Server-side license generation
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	privateKey, err := ReadPrivateKeyContents(privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
	ld := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	encodedLicense, err := signAndEncode(privateKey, ld)
	if err != nil {
		t.Error(err)
	}
	falsifiedEncodedLicense, err := falsifyEncodedLicense(encodedLicense)
	if err != nil {
		t.Error(err)
	}
	err = ioutil.WriteFile("myrtea-license.key", []byte(falsifiedEncodedLicense), 0644)
	if err != nil {
		t.Error(err)
	}

	// Client-side license validation
	publicKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key-pub.pem")
	if err != nil {
		t.Error(err)
	}
	publicKey, err := ReadPublicKeyContents(publicKeyBytes)
	if err != nil {
		t.Error(err)
	}
	readLicense, err := ioutil.ReadFile("myrtea-license.key")
	if err != nil {
		t.Error(err)
	}
	_, err = decodeAndVerify(publicKey, string(readLicense))
	if err == nil {
		t.Error("This license is not supposed to be valid (must raise a error : 'crypto/rsa: verification error')")
	}
	err = os.Remove("myrtea-license.key")
	if err != nil {
		t.Error(err)
	}
}

func falsifyEncodedLicense(str string) (string, error) {
	license, err := decode(str)
	if err != nil {
		return "", err
	}
	flicense := falsifyLicense(license)
	fstr, err := encode(flicense)
	if err != nil {
		return "", err
	}
	return fstr, nil
}

func falsifyLicense(l *license) *license {
	i, err := gobDecode(l.Payload)
	if err != nil {
		log.Println(err)
		return nil
	}
	data := i.(MyrteaLicenseData)
	data.Company = "FALSIFIED"

	fakePayload, err := gobEncode(data)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &license{fakePayload, l.Signature}
}
