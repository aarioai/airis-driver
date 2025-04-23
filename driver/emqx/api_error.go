package emqx

import "github.com/aarioai/airis/aa/ae"

type EmqxAPIError struct {
	Code   string `json:"code"`
	Reason string `json:"reason"`
}

func NewError(err EmqxAPIError) *ae.Error {
	if err.Code == "" {
		return nil
	}
	return ae.NewE("code: %s, reason: %s", err.Code, err.Reason)
}
