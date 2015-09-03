/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package framework

import (
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "net/url"
)

type ApiRequest struct {
    url    string
    params map[string]string
}

func NewApiRequest(url string) *ApiRequest {
    return &ApiRequest{
        url:    url,
        params: make(map[string]string),
    }
}

func (r *ApiRequest) AddParam(key string, value interface{}) {
    str := fmt.Sprintf("%s", value)
    if str != "" {
        r.params[key] = str
    }
}

func (r *ApiRequest) Get() *ApiResponse {
    values := url.Values{}
    for key, value := range r.params {
        values.Set(key, value)
    }
    queryString := values.Encode()

    url := fmt.Sprintf("%s?%s", r.url, queryString)
    response, err := http.Get(url)
    if err != nil {
        return &ApiResponse{false, err.Error()}
    }

    responseBody, err := ioutil.ReadAll(response.Body)
    if err != nil {
        return &ApiResponse{false, err.Error()}
    }

    apiResponse := new(ApiResponse)
    err = json.Unmarshal(responseBody, apiResponse)
    if err != nil {
        return &ApiResponse{false, err.Error()}
    }

    return apiResponse
}

type ApiResponse struct {
    Success bool
    Message string
}

func NewApiResponse(success bool, message string) *ApiResponse {
    return &ApiResponse{
        Success: success,
        Message: message,
    }
}
