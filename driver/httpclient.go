package driver

import (
	"crypto/tls"
	"errors"
	"github.com/aarioai/airis/aa"
	"github.com/aarioai/airis/aa/ae"
	"github.com/aarioai/airis/pkg/types"
	"net"
	"net/http"
	"strings"
	"time"
)

type HttpClientConfig struct {
	// http.Client
	Timeout time.Duration `json:"timeout"`

	// net.Dialer
	DialTimeout time.Duration `json:"dial_timeout"`
	//DialDeadline      time.Time     `json:"dial_deadline"`
	DialDualStack     bool          `json:"dial_dual_stack"`
	DialFallbackDelay time.Duration `json:"dial_fallback_delay"`
	DialKeepAlive     time.Duration `json:"dial_keep_alive"`
	//net.KeepAliveConfig
	DialKeepAliveEnable   bool          `json:"dial_keep_alive_enable"`
	DialKeepAliveIdle     time.Duration `json:"dial_keep_alive_idle"`
	DialKeepAliveInterval time.Duration `json:"dial_keep_alive_interval"`
	DialKeepAliveCount    int           `json:"dial_keep_alive_count"`

	// http.Transport
	DisableKeepAlives      bool          `json:"disable_keep_alives"`
	DisableCompression     bool          `json:"disable_compression"`
	MaxIdleConns           int           `json:"max_idle_conns"`
	MaxIdleConnsPerHost    int           `json:"max_idle_conns_per_host"`
	MaxConnsPerHost        int           `json:"max_conns_per_host"`
	IdleConnTimeout        time.Duration `json:"idle_conn_timeout"`
	ResponseHeaderTimeout  time.Duration `json:"response_header_timeout"`
	ExpectContinueTimeout  time.Duration `json:"expect_continue_timeout"`
	MaxResponseHeaderBytes int64         `json:"max_response_header_bytes"`
	WriteBufferSize        int           `json:"write_buffer_size"`
	ReadBufferSize         int           `json:"read_buffer_size"`
	ForceAttemptHTTP2      bool          `json:"force_attempt_http2"`

	TLSHandshakeTimeout time.Duration `json:"tls_handshake_timeout"`
	// tls.Config
	TLSNextProtos                     []string                 `json:"tls_next_protos"`
	TLSServerName                     string                   `json:"tls_server_name"`
	TLSClientAuth                     tls.ClientAuthType       `json:"tls_client_auth"`
	TLSInsecureSkipVerify             bool                     `json:"tls_insecure_skip_verify"`
	TLSCipherSuites                   []uint16                 `json:"tls_cipher_suites"`
	TLSSessionTicketsDisabled         bool                     `json:"tls_session_tickets_disabled"`
	TLSMinVersion                     uint16                   `json:"tls_min_version"`
	TLSMaxVersion                     uint16                   `json:"tls_max_version"`
	TLSCurvePreferences               []tls.CurveID            `json:"tls_curve_preferences"`
	TLSDynamicRecordSizingDisabled    bool                     `json:"tls_dynamic_record_sizing_disabled"`
	TLSRenegotiation                  tls.RenegotiationSupport `json:"tls_renegotiation"`
	TLSEncryptedClientHelloConfigList []byte                   `json:"tls_encrypted_client_hello_config_list"`

	// tls.Certificate  tls.LoadX509KeyPair(certFile, keyFile)
	TLSCertificateFilePairs [][2]string `json:"tls_certificate_file_pairs"`
}

func (c HttpClientConfig) Dialer() *net.Dialer {
	return &net.Dialer{
		Timeout: c.DialTimeout,
		//Deadline: c.DialDeadline,
		//LocalAddr:     nil,
		//DualStack:     false,
		FallbackDelay: c.DialFallbackDelay,
		KeepAlive:     c.DialKeepAlive,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   c.DialKeepAliveEnable,
			Idle:     c.DialKeepAliveIdle,
			Interval: c.DialKeepAliveInterval,
			Count:    c.DialKeepAliveCount,
		},
		//Resolver:       nil,
		//Cancel:         nil,
		//Control:        nil,
		//ControlContext: nil,
	}
}

func (c HttpClientConfig) TLSCertificates() ([]tls.Certificate, error) {
	if len(c.TLSCertificateFilePairs) == 0 {
		return nil, nil
	}
	certs := make([]tls.Certificate, len(c.TLSCertificateFilePairs))
	for i, pair := range c.TLSCertificateFilePairs {
		cert, err := tls.LoadX509KeyPair(pair[0], pair[1])
		if err != nil {
			return nil, err
		}
		certs[i] = cert
	}
	return certs, nil
}

func (c HttpClientConfig) TLSClientConfig() (*tls.Config, error) {
	certs, err := c.TLSCertificates()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		NextProtos:                     c.TLSNextProtos,
		ServerName:                     c.TLSServerName,
		ClientAuth:                     c.TLSClientAuth,
		InsecureSkipVerify:             c.TLSInsecureSkipVerify,
		CipherSuites:                   c.TLSCipherSuites,
		SessionTicketsDisabled:         c.TLSSessionTicketsDisabled,
		MinVersion:                     c.TLSMinVersion,
		MaxVersion:                     c.TLSMaxVersion,
		CurvePreferences:               c.TLSCurvePreferences,
		DynamicRecordSizingDisabled:    c.TLSDynamicRecordSizingDisabled,
		Renegotiation:                  c.TLSRenegotiation,
		EncryptedClientHelloConfigList: c.TLSEncryptedClientHelloConfigList,
		Certificates:                   certs,
	}, nil
}

func (c HttpClientConfig) Transport() (*http.Transport, error) {
	tlsConfig, err := c.TLSClientConfig()
	if err != nil {
		return nil, err
	}
	return &http.Transport{
		DialContext:         c.Dialer().DialContext,
		TLSHandshakeTimeout: c.TLSHandshakeTimeout,
		TLSClientConfig:     tlsConfig,
	}, nil
}

func NewHttpClient(app *aa.App, section string) (*http.Client, *ae.Error) {
	c, err := ParseHttpClientConfig(app, section)
	if err != nil {
		return nil, newConfigError(section, err)
	}
	transport, err := c.Transport()
	if err != nil {
		return nil, newConfigError(section, err)
	}
	return &http.Client{
		Transport:     transport,
		CheckRedirect: nil,
		Jar:           nil,
		Timeout:       c.Timeout,
	}, nil
}

func ParseHttpClientConfig(app *aa.App, section string) (HttpClientConfig, error) {
	timeout, _ := tryGetSectionCfg(app, "httpc", section, "timeout")
	dialTimeout, _ := tryGetSectionCfg(app, "httpc", section, "dial_timeout")
	dialDualStack, _ := tryGetSectionCfg(app, "httpc", section, "dial_dual_stack")
	dialFallbackDelay, _ := tryGetSectionCfg(app, "httpc", section, "dial_fallback_delay")
	dialKeepAlive, _ := tryGetSectionCfg(app, "httpc", section, "dial_keep_alive")
	dialKeepAliveEnable, _ := tryGetSectionCfg(app, "httpc", section, "dial_keep_alive_enable")
	dialKeepAliveIdle, _ := tryGetSectionCfg(app, "httpc", section, "dial_keep_alive_idle")
	dialKeepAliveInterval, _ := tryGetSectionCfg(app, "httpc", section, "dial_keep_alive_interval")
	dialKeepAliveCount, _ := tryGetSectionCfg(app, "httpc", section, "dial_keep_alive_count")

	disableKeepAlives, _ := tryGetSectionCfg(app, "httpc", section, "disable_keep_alives")
	disableCompression, _ := tryGetSectionCfg(app, "httpc", section, "disable_compression")
	maxIdleConns, _ := tryGetSectionCfg(app, "httpc", section, "max_idle_conns")
	maxIdleConnsPerHost, _ := tryGetSectionCfg(app, "httpc", section, "max_idle_conns_per_host")
	maxConnsPerHost, _ := tryGetSectionCfg(app, "httpc", section, "max_conns_per_host")
	idleConnTimeout, _ := tryGetSectionCfg(app, "httpc", section, "idle_conn_timeout")
	responseHeaderTimeout, _ := tryGetSectionCfg(app, "httpc", section, "response_header_timeout")
	expectContinueTimeout, _ := tryGetSectionCfg(app, "httpc", section, "expect_continue_timeout")
	maxResponseHeaderBytes, _ := tryGetSectionCfg(app, "httpc", section, "max_response_header_bytes")
	writeBufferSize, _ := tryGetSectionCfg(app, "httpc", section, "write_buffer_size")
	readBufferSize, _ := tryGetSectionCfg(app, "httpc", section, "read_buffer_size")
	forceAttemptHTTP2, _ := tryGetSectionCfg(app, "httpc", section, "force_attempt_http2")

	tlsHandshakeTimeout, _ := tryGetSectionCfg(app, "httpc", section, "tls_handshake_timeout")
	tlsNextProtos, _ := tryGetSectionCfg(app, "httpc", section, "tls_next_protos")
	tlsServerName, _ := tryGetSectionCfg(app, "httpc", section, "tls_server_name")
	tlsClientAuth, _ := tryGetSectionCfg(app, "httpc", section, "tls_client_auth")
	tlsInsecureSkipVerify, _ := tryGetSectionCfg(app, "httpc", section, "tls_insecure_skip_verify")
	tlsCipherSuites, _ := tryGetSectionCfg(app, "httpc", section, "tls_cipher_suites")
	tlsSessionTicketsDisabled, _ := tryGetSectionCfg(app, "httpc", section, "tls_session_tickets_disabled")
	tlsMinVersion, _ := tryGetSectionCfg(app, "httpc", section, "tls_min_version")
	tlsMaxVersion, _ := tryGetSectionCfg(app, "httpc", section, "tls_max_version")
	tlsCurvePreferences, _ := tryGetSectionCfg(app, "httpc", section, "tls_curve_preferences")
	tlsDynamicRecordSizingDisabled, _ := tryGetSectionCfg(app, "httpc", section, "tls_dynamic_record_sizing_disabled")
	tlsRenegotiation, _ := tryGetSectionCfg(app, "httpc", section, "tls_renegotiation")
	tlsEncryptedClientHelloConfigList, _ := tryGetSectionCfg(app, "httpc", section, "tls_encrypted_client_hello_config_list")
	tlsCertificateFilePairs, _ := tryGetSectionCfg(app, "httpc", section, "tls_certificate_file_pairs")

	dialKeepAliveCountN, _ := types.ParseInt(dialKeepAliveCount)
	maxIdleConnsN, _ := types.ParseInt(maxIdleConns)
	maxIdleConnsPerHostN, _ := types.ParseInt(maxIdleConnsPerHost)
	maxConnsPerHostN, _ := types.ParseInt(maxConnsPerHost)
	maxResponseHeaderBytesN, _ := types.ParseInt64(maxResponseHeaderBytes)
	writeBufferSizeN, _ := types.ParseInt(writeBufferSize)
	readBufferSizeN, _ := types.ParseInt(readBufferSize)
	tlsClientAuthN, _ := types.ParseInt(tlsClientAuth)

	tlsCurvePreferencesN := parseUint16s(tlsCurvePreferences, ",")
	var tlsCurveIDs []tls.CurveID
	if len(tlsCurvePreferences) > 0 {
		tlsCurveIDs = make([]tls.CurveID, len(tlsCurvePreferencesN))
		for i, tlsCurveID := range tlsCurvePreferencesN {
			tlsCurveIDs[i] = tls.CurveID(tlsCurveID)
		}
	}

	tlsRenegotiationN, _ := types.ParseInt(tlsRenegotiation)
	var tlsEncryptedClientHelloConfigListN []byte
	tlsEncryptedClientHelloConfigList = strings.Trim(tlsEncryptedClientHelloConfigList, " ")
	if tlsEncryptedClientHelloConfigList != "" {
		tlsEncryptedClientHelloConfigListN = []byte(tlsEncryptedClientHelloConfigList)
	}
	//   /a/df/a.crt,/a/df/a.key;/a/df/b.crt,/a/df/b.key
	var tlsCertFilePairs [][2]string
	tlsCertFiles := parseStrings(tlsCertificateFilePairs, ";")
	if len(tlsCertFiles) > 0 {
		tlsCertFilePairs = make([][2]string, 0, len(tlsCertFiles))
		for _, tcf := range tlsCertFiles {
			pair := parseStrings(tcf, ",")
			if len(pair) == 0 {
				continue
			}
			if len(pair) != 2 {
				return HttpClientConfig{}, errors.New("tls_certificate_file_pairs invalid. config format should be: <certFile>,<keyFile>[;<certFile>,<keyFile>...]")
			}
			tlsCertFilePairs = append(tlsCertFilePairs, [2]string{pair[0], pair[1]})
		}
	}

	return HttpClientConfig{
		Timeout:                           types.ParseDuration(timeout),
		DialTimeout:                       types.ParseDuration(dialTimeout),
		DialDualStack:                     types.ToBool(dialDualStack),
		DialFallbackDelay:                 types.ParseDuration(dialFallbackDelay),
		DialKeepAlive:                     types.ParseDuration(dialKeepAlive),
		DialKeepAliveEnable:               types.ToBool(dialKeepAliveEnable),
		DialKeepAliveIdle:                 types.ParseDuration(dialKeepAliveIdle),
		DialKeepAliveInterval:             types.ParseDuration(dialKeepAliveInterval),
		DialKeepAliveCount:                dialKeepAliveCountN,
		DisableKeepAlives:                 types.ToBool(disableKeepAlives),
		DisableCompression:                types.ToBool(disableCompression),
		MaxIdleConns:                      maxIdleConnsN,
		MaxIdleConnsPerHost:               maxIdleConnsPerHostN,
		MaxConnsPerHost:                   maxConnsPerHostN,
		IdleConnTimeout:                   types.ParseDuration(idleConnTimeout),
		ResponseHeaderTimeout:             types.ParseDuration(responseHeaderTimeout),
		ExpectContinueTimeout:             types.ParseDuration(expectContinueTimeout),
		MaxResponseHeaderBytes:            maxResponseHeaderBytesN,
		WriteBufferSize:                   writeBufferSizeN,
		ReadBufferSize:                    readBufferSizeN,
		ForceAttemptHTTP2:                 types.ToBool(forceAttemptHTTP2),
		TLSHandshakeTimeout:               types.ParseDuration(tlsHandshakeTimeout),
		TLSNextProtos:                     parseStrings(tlsNextProtos, ","),
		TLSServerName:                     tlsServerName,
		TLSClientAuth:                     tls.ClientAuthType(tlsClientAuthN),
		TLSInsecureSkipVerify:             types.ToBool(tlsInsecureSkipVerify),
		TLSCipherSuites:                   parseUint16s(tlsCipherSuites, ","),
		TLSSessionTicketsDisabled:         types.ToBool(tlsSessionTicketsDisabled),
		TLSMinVersion:                     types.ToUint16(tlsMinVersion),
		TLSMaxVersion:                     types.ToUint16(tlsMaxVersion),
		TLSCurvePreferences:               tlsCurveIDs,
		TLSDynamicRecordSizingDisabled:    types.ToBool(tlsDynamicRecordSizingDisabled),
		TLSRenegotiation:                  tls.RenegotiationSupport(tlsRenegotiationN),
		TLSEncryptedClientHelloConfigList: tlsEncryptedClientHelloConfigListN,
		TLSCertificateFilePairs:           tlsCertFilePairs,
	}, nil
}

func NewHttpError(err error) *ae.Error {
	return ae.NewError(err)
}
