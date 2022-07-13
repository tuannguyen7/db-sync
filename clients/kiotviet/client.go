package kiotviet

import (
	"bytes"
	"context"
	"db-sync/config"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	AuthEndpoint = "https://id.kiotviet.vn/connect/token"
	HOST         = "https://public.kiotapi.com/"
	WebHost      = "https://api-man.kiotviet.vn/api"
)

type Client struct {
	httpClient *http.Client

	// Web login secrets
	userName string
	password string

	// token
	accessToken    string
	webAccessToken string
	// cookies
	webCookies []*http.Cookie

	// API secrets
	clientID     string
	clientSecret string
	retailer     string
}

type callFunc func() (*http.Request, error)
type setAuthFunc func(ctx context.Context, req *http.Request, refreshToken bool) error

func NewClient() (*Client, error) {
	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    30 * time.Second,
		DisableCompression: true,
	}
	client := &http.Client{Transport: tr}

	return &Client{
		httpClient:   client,
		clientID:     config.KiotVietClientID,
		clientSecret: config.KiotVietClientSecret,
		retailer:     config.KiotVietRetailer,
		userName:     config.KiotVietUserName,
		password:     config.KiotVietPassWord,
	}, nil
}

func (c *Client) getWebAccessToken(ctx context.Context) (*WebAccessToken, error) {
	var webAccessToken WebAccessToken
	url := "https://api-man.kiotviet.vn/api/account/login?quan-ly=true"
	credential := map[string]interface{}{
		"UserName":       c.userName,
		"Password":       c.password,
		"LatestBranchId": 14628,
		"RememberMe":     true,
		"ShowCaptcha":    false,
	}
	payload := map[string]interface{}{
		"model": credential,
	}

	jsonValue, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Retailer", c.retailer)
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return nil, fmt.Errorf("error logging via web Kiotviet")
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(body, &webAccessToken); err != nil {
		return nil, err
	}

	webAccessToken.Cookies = resp.Cookies()
	return &webAccessToken, nil
}

func (c *Client) getAccessToken(ctx context.Context) (*AccessToken, error) {
	var token AccessToken
	form := url.Values{
		"scopes":        {"PublicApi.Access"},
		"grant_type":    {"client_credentials"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}

	resp, err := http.PostForm(AuthEndpoint, form)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		log.WithFields(log.Fields{
			"resp":       string(body),
			"statusCode": resp.StatusCode,
		}).Errorln("error getting access token")
		return nil, fmt.Errorf("error getting access token")
	}

	err = json.Unmarshal(body, &token)
	if err != nil {
		return nil, err
	}

	return &token, nil
}

func (c *Client) setAuthHeaders(ctx context.Context, req *http.Request, refreshToken bool) error {
	if refreshToken {
		accessToken, err := c.getAccessToken(ctx)
		if err != nil {
			return err
		}
		c.accessToken = accessToken.AccessToken
	}

	req.Header.Add("Retailer", c.retailer)
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.accessToken))
	return nil
}

func (c *Client) setWebAuthHeaders(ctx context.Context, req *http.Request, refreshToken bool) error {
	if refreshToken || c.webAccessToken == "" {
		accessToken, err := c.getWebAccessToken(ctx)
		if err != nil {
			return err
		}
		c.webAccessToken = accessToken.Token
		c.webCookies = accessToken.Cookies
	}

	req.Header.Add("retailer", c.retailer)
	req.Header.Add("authorization", fmt.Sprintf("Bearer %s", c.webAccessToken))
	for _, cookie := range c.webCookies {
		req.AddCookie(cookie)
	}

	return nil
}

func (c *Client) try(ctx context.Context, fn callFunc, authFunc setAuthFunc) (*http.Response, error) {
	req, err := fn()
	if err != nil {
		return nil, err
	}

	if err = authFunc(ctx, req, false); err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 401 {
		return resp, err
	}

	// If we get a response with 401(Unauthorized) status, we refresh the access token then make the call again
	if err = resp.Body.Close(); err != nil {
		return nil, err
	}

	req, err = fn()
	if err != nil {
		return nil, err
	}

	if err = authFunc(ctx, req, true); err != nil {
		return nil, err
	}

	return c.httpClient.Do(req)
}

func (c *Client) ListTransfers(ctx context.Context, limit int, offset int) (*TransferPage, error) {

	var page TransferPage
	var fn = func() (*http.Request, error) {
		req, err := http.NewRequest("GET", HOST+"transfers", nil)
		q := req.URL.Query()
		q.Add("pageSize", strconv.Itoa(limit))
		q.Add("currentItem", strconv.Itoa(offset))
		q.Add("orderBy", "id")
		q.Add("orderDirection", "DESC")
		req.URL.RawQuery = q.Encode()
		return req, err
	}

	resp, err := c.try(ctx, fn, c.setAuthHeaders)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &page)
	if err != nil {
		return nil, err
	}

	return &page, nil
}

func (c *Client) GetTransferDetailWeb(ctx context.Context, transferID int64) (*WebTransferDetailResp, error) {
	var detailResp WebTransferDetailResp
	var fn = func() (*http.Request, error) {
		endpoint := fmt.Sprintf("%s/transferDetails/%d", WebHost, transferID)
		req, err := http.NewRequest("GET", endpoint, nil)
		q := req.URL.Query()
		q.Add("Includes", "Product")
		req.URL.RawQuery = q.Encode()
		return req, err
	}

	resp, err := c.try(ctx, fn, c.setWebAuthHeaders)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &detailResp)
	if err != nil {
		return nil, err
	}

	return &detailResp, nil
}
