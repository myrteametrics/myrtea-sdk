# License package

## License Generation

```go
package main

import (
	"fmt"
	"io/ioutil"
	"time"

	"github.com/myrteametrics/myrtea-sdk/v5/license"
)

func main() {
	data := license.NewMyrteaLicenseData("Myrtea Client", "Test Projet", "Myrtea",
		"myrtea@myrteametrics.com", 5*time.Second, "Myrtea Issuer")
	license, err := license.Generate(data, "license/testdata/license-signing-key.pem")
	if err != nil {
        fmt.Println(err)
        return
	}

	err = ioutil.WriteFile("myrtea-license.key", []byte(license), 0644)
	if err != nil {
        fmt.Println(err)
        return
    }
    // [...]
}
```


## License Validation

```go
package main

import (
	"fmt"

	"github.com/myrteametrics/myrtea-sdk/v5/license"
)

var publicKey = []byte(`
-----BEGIN PUBLIC KEY-----
MIICITANBgkqhk[...]
-----END PUBLIC KEY-----
`)

func main() {
	data, err := license.Verify("myrtea-license.key", publicKey)
	if err != nil {
        fmt.Println(err)
        return
	}
    fmt.Println(data)
    // [...]
}
```