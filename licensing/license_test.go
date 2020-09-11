package license

import (
	"io/ioutil"
	"testing"
	"time"
)

func TestLicenseGenerate(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	data := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	_, err = Generate(data, privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
}

func TestLicenseGenerateInvalidPrivateKey(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/invalid_key.pem")
	if err != nil {
		t.Error(err)
	}
	data := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	_, err = Generate(data, privateKeyBytes)
	if err == nil {
		t.Error("Private key should be invalid")
	}
}

func TestLicenseVerify(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	data := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	license, err := Generate(data, privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
	publicKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key-pub.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = Verify([]byte(license), publicKeyBytes)
	if err != nil {
		t.Error(err)
	}
}

func TestLicenseVerifyInvalidPublicKey(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	data := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	license, err := Generate(data, privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
	publicKeyBytes, err := ioutil.ReadFile("testdata/invalid_key.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = Verify([]byte(license), publicKeyBytes)
	if err == nil {
		t.Error("Public key should not be valid")
	}
}

func TestLicenseInvalidKey(t *testing.T) {
	privateKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key.pem")
	if err != nil {
		t.Error(err)
	}
	data := NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea", "myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	license, err := Generate(data, privateKeyBytes)
	if err != nil {
		t.Error(err)
	}
	license = license[:len(license)-20] // Corrupt license string
	publicKeyBytes, err := ioutil.ReadFile("testdata/license-signing-key-pub.pem")
	if err != nil {
		t.Error(err)
	}
	_, err = Verify([]byte(license), publicKeyBytes)
	if err == nil {
		t.Error("License must not be valid")
	}
}
