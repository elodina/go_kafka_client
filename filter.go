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

package go_kafka_client

import (
	"regexp"
)

const (
	offsetsTopicName = "__consumer_offsets"
)

//WhiteList is a topic filter that will match every topic for a given regex
type WhiteList struct {
	rawRegex      string
	compiledRegex *regexp.Regexp
}

func (wl *WhiteList) Regex() string {
	return wl.rawRegex
}

func (wl *WhiteList) TopicAllowed(topic string, excludeInternalTopics bool) bool {
	return wl.compiledRegex.MatchString(topic) && !(topic == offsetsTopicName && excludeInternalTopics)
}

//Creates a new WhiteList topic filter for a given regex
func NewWhiteList(regex string) *WhiteList {
	cregexp, err := regexp.Compile(regex)
	if err != nil {
		panic(err)
	}
	return &WhiteList{
		rawRegex:      regex,
		compiledRegex: cregexp,
	}
}

//BlackList is a topic filter that will match every topic that does not match a given regex
type BlackList struct {
	rawRegex      string
	compiledRegex *regexp.Regexp
}

func (bl *BlackList) Regex() string {
	return bl.rawRegex
}

func (bl *BlackList) TopicAllowed(topic string, excludeInternalTopics bool) bool {
	return !bl.compiledRegex.MatchString(topic) && !(topic == offsetsTopicName && excludeInternalTopics)
}

//Creates a new BlackList topic filter for a given regex
func NewBlackList(regex string) *BlackList {
	cregexp, err := regexp.Compile(regex)
	if err != nil {
		panic(err)
	}
	return &BlackList{
		rawRegex:      regex,
		compiledRegex: cregexp,
	}
}
