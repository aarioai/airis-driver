package emqx

import (
	"bytes"
	"encoding/json"
	"github.com/aarioai/airis/aa/ae"
	"net/http"
)

type EmqxAPI struct {
	Host     string
	username string
	password string
}

func NewEmqxAPI(host string) *EmqxAPI {
	return &EmqxAPI{
		Host: host,
	}
}

func (s *EmqxAPI) WithBasicAuth(username, password string) *EmqxAPI {
	s.username = username
	s.password = password
	return s
}

func (s *EmqxAPI) Request(target any, method, path string) (*ae.Error, error) {
	link := s.Host + path
	r, err := http.NewRequest(method, link, nil)
	if err != nil {
		return nil, err
	}
	if s.username != "" {
		r.SetBasicAuth(s.username, s.password)
	}
	r.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	if _, err = buf.ReadFrom(resp.Body); err != nil {
		return nil, err
	}
	if err = json.Unmarshal(buf.Bytes(), &target); err == nil {
		return nil, nil
	}
	var emqxError EmqxAPIError
	err = json.Unmarshal(buf.Bytes(), &emqxError)
	return NewError(emqxError), nil
}
