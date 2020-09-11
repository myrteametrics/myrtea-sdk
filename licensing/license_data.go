package license

import (
	"fmt"
	"time"
)

// ILicenseData is a top interface for any potential licensing data
// which must be validated
type ILicenseData interface {
	Validate() (bool, error)
}

// MyrteaLicenseData is the default data structure for Myrtea licenses
type MyrteaLicenseData struct {
	Serial       string
	Company      string
	Project      string
	ContactName  string
	ContactEmail string
	DateIssued   time.Time
	DateExpires  time.Time
	IssuedBy     string
}

// NewMyrteaLicenseData returns a new filled MyrteaLicenseData object
func NewMyrteaLicenseData(company string, project string, contactName string, contactEmail string,
	duration time.Duration, issuedBy string) MyrteaLicenseData {
	return MyrteaLicenseData{"1", company, project, contactName, contactEmail, time.Now(), time.Now().Add(duration), issuedBy}
}

// Validate checks if the license is valid. A Myrtea license is valid if :
// * It's not expired (DateExpires < Now)
func (ld MyrteaLicenseData) Validate() (bool, error) {
	if !ld.DateExpires.After(time.Now()) {
		return false, fmt.Errorf("Expired since %s", ld.DateExpires.UTC())
	}
	return true, nil
}
