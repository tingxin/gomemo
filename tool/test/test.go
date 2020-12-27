package test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/stretchr/testify/suite"
)

//HTTPServer used to represent http server
type HTTPServer interface {
	ServeHTTP(httpwriter http.ResponseWriter, httpRequest *http.Request)
}

// NewAPITest used to
func NewAPITest(reqs []APITestRequest, assertFn func(*suite.Suite, APITest)) *APITest {
	return &APITest{
		Requests:    reqs,
		AssertionFn: assertFn,
	}
}

// APITest used to
type APITest struct {
	Requests    []APITestRequest
	Responses   []APITestResponse
	AssertionFn func(*suite.Suite, APITest)
}

// Run used to
func (ref *APITest) Run(s *suite.Suite, server HTTPServer) {
	for _, req := range ref.Requests {
		if req.PreReqFn != nil {
			req.PreReqFn(*ref, req.Request)
		}
		ref.Responses = append(ref.Responses, RunRequest(s, server, req.Request))
	}
	ref.AssertionFn(s, *ref)
}

// APITestRequest used to
type APITestRequest struct {
	Request  *http.Request
	PreReqFn func(APITest, *http.Request)
}

// APITestResponse used to
type APITestResponse struct {
	HeaderMap http.Header
	Code      int
	BodyJSON  interface{}
	BodyText  string
}

// BuildRequest used to build a new http request
func BuildRequest(s *suite.Suite, method, path string, bodyJSONObj interface{}) *http.Request {
	body, err := json.Marshal(bodyJSONObj)
	s.Nil(err, "Couldn't marshal bodyJSONObj to JSON.")
	req, err := http.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	s.Nil(err, "Couldn't build request")
	return req
}

// ReplaceRequestBody used to replace request body
func ReplaceRequestBody(req *http.Request, oldStr, newStr string) *http.Request {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		panic("Developer error: " + err.Error())
	}
	bodyStr := string(body)
	bodyStr = strings.Replace(bodyStr, oldStr, newStr, 1)
	req.Body = ioutil.NopCloser(strings.NewReader(bodyStr))
	return req
}

// RunRequest used to test
func RunRequest(s *suite.Suite, server HTTPServer, req *http.Request) APITestResponse {
	resp := httptest.NewRecorder()
	server.ServeHTTP(resp, req)
	data, err := ioutil.ReadAll(resp.Body)
	s.Nil(err, "Couldn't read response body for req: "+req.URL.String())
	fmt.Printf("Response for %s %q: %d %s", req.Method, req.URL.String(), resp.Code, string(data))
	testResponse := APITestResponse{Code: resp.Code, BodyText: string(data), HeaderMap: resp.HeaderMap}
	switch resp.HeaderMap.Get("Content-Type") {
	case "application/json":
		result := map[string]interface{}{}
		if err = json.Unmarshal(data, &result); err != nil {
			arrResult := make([]map[string]interface{}, 0)
			err = json.Unmarshal(data, &arrResult)
			s.Nil(err, "Couldn't parse response body for req: "+req.URL.String())
			testResponse.BodyJSON = arrResult
			return testResponse
		}
		testResponse.BodyJSON = result
	}
	return testResponse
}
