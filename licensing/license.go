package license

import (
	"encoding/gob"
)

// Generate encrypt and sign a license with a private key
func Generate(licenseData ILicenseData, privateKeyData []byte) (string, error) {
	gob.Register(MyrteaLicenseData{})

	privateKey, err := ReadPrivateKeyContents(privateKeyData)
	if err != nil {
		return "", err
	}

	encodedLicense, err := signAndEncode(privateKey, licenseData)
	if err != nil {
		return "", err
	}

	return encodedLicense, nil
}

// Verify reads, decrypts and checks validity of a license file
func Verify(licenseContent []byte, publicKeyData []byte) (ILicenseData, error) {
	gob.Register(MyrteaLicenseData{})

	publicKey, err := ReadPublicKeyContents(publicKeyData)
	if err != nil {
		return nil, err
	}

	decodedData, err := decodeAndVerify(publicKey, string(licenseContent))
	if err != nil {
		return nil, err
	}

	decodedLicense := decodedData.(ILicenseData)
	if ok, err := decodedLicense.Validate(); !ok {
		return decodedLicense, err
	}
	return decodedLicense, nil
}
