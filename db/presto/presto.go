package presto

import (
	"crypto/tls"
	"crypto/x509"
	_ "database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/prestodb/presto-go-client/presto"
)

// GetConnStr used to build the connection string
func GetConnStr(address, user, pass, catalog string) string {
	format := "https://%s:%s@%s?custom_client=insight&catalog=%s"
	connStr := fmt.Sprintf(format, user, pass, address, catalog)
	return connStr
}

// SetPrestoClient used to register default custom http client
func SetPrestoClient(clientKey, pemPath string) {
	pem, err := ioutil.ReadFile(pemPath)
	if err != nil {
		panic(fmt.Errorf("open %s met %s", "presto", err.Error()))
	}
	cer := x509.NewCertPool()
	ok := cer.AppendCertsFromPEM(pem)
	if !ok {
		fmt.Println("read pem failed!")
		return
	}
	c := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: cer,
			},
		},
	}
	err = presto.RegisterCustomClient(clientKey, c)
	if err != nil {
		fmt.Print(err)
	}
}
