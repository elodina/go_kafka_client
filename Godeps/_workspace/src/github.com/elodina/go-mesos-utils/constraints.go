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

package utils

import (
	"encoding/json"
	"fmt"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"math"
	"regexp"
	"strconv"
	"strings"
)

type Constraints map[string][]Constraint

func (c Constraints) MarshalJSON() ([]byte, error) {
	constraintsMap := make(map[string][]string)
	for attr, constraints := range c {
		constraintsMap[attr] = make([]string, len(constraints))
		for idx, constraint := range constraints {
			constraintsMap[attr][idx] = fmt.Sprintf("%s", constraint)
		}
	}

	return json.Marshal(constraintsMap)
}

func (c Constraints) UnmarshalJSON(data []byte) error {
	if c == nil {
		c = make(Constraints)
	}

	constraintsMap := make(map[string][]string)
	err := json.Unmarshal(data, &constraintsMap)
	if err != nil {
		return err
	}

	for attr, constraints := range constraintsMap {
		c[attr] = make([]Constraint, len(constraints))
		for idx, constraintString := range constraints {
			constraint, err := ParseConstraint(constraintString)
			if err != nil {
				return err
			}
			c[attr][idx] = constraint
		}
	}

	return nil
}

type Constraint interface {
	Matches(value string, values []string) bool
}

func ParseConstraint(value string) (Constraint, error) {
	switch {
	case strings.HasPrefix(value, "like"):
		return NewLikeConstraint(value[len("like:"):])
	case strings.HasPrefix(value, "unlike"):
		return NewUnlikeConstraint(value[len("unlike:"):])
	case value == "unique":
		return NewUniqueConstraint(), nil
	case strings.HasPrefix(value, "cluster"):
		{
			tail := value[len("cluster"):]
			if tail == "" {
				return NewClusterConstraint(""), nil
			} else {
				return NewClusterConstraint(tail[1:]), nil
			}
		}
	case strings.HasPrefix(value, "groupBy"):
		{
			tail := value[len("groupBy"):]
			if strings.HasPrefix(tail, ":") {
				groups, err := strconv.Atoi(tail[1:])
				if err != nil {
					return nil, fmt.Errorf("Invalid constraint: %s", value)
				}
				return NewGroupByConstraint(groups), nil
			} else {
				return NewGroupByConstraint(1), nil
			}
		}
	default:
		return nil, fmt.Errorf("Unsupported constraint: %s", value)
	}
}

func MustParseConstraint(value string) Constraint {
	constraint, err := ParseConstraint(value)
	if err != nil {
		panic(err)
	}

	return constraint
}

type Like struct {
	regex   string
	pattern *regexp.Regexp
}

func NewLikeConstraint(regex string) (*Like, error) {
	pattern, err := regexp.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("Invalid like: %s", err)
	}

	return &Like{
		regex:   regex,
		pattern: pattern,
	}, nil
}

func (l *Like) Matches(value string, values []string) bool {
	return l.pattern.MatchString(value)
}

func (l *Like) String() string {
	return fmt.Sprintf("like:%s", l.regex)
}

type Unlike struct {
	regex   string
	pattern *regexp.Regexp
}

func NewUnlikeConstraint(regex string) (*Unlike, error) {
	pattern, err := regexp.Compile(regex)
	if err != nil {
		return nil, fmt.Errorf("Invalid unlike: %s", err)
	}

	return &Unlike{
		regex:   regex,
		pattern: pattern,
	}, nil
}

func (u *Unlike) Matches(value string, values []string) bool {
	return !u.pattern.MatchString(value)
}

func (u *Unlike) String() string {
	return fmt.Sprintf("unlike:%s", u.regex)
}

type Unique struct{}

func NewUniqueConstraint() *Unique {
	return new(Unique)
}

func (u *Unique) Matches(value string, values []string) bool {
	for _, v := range values {
		if value == v {
			return false
		}
	}

	return true
}

func (u *Unique) String() string {
	return "unique"
}

type Cluster struct {
	value string
}

func NewClusterConstraint(value string) *Cluster {
	return &Cluster{
		value: value,
	}
}

func (c *Cluster) Matches(value string, values []string) bool {
	if c.value != "" {
		return c.value == value
	} else {
		return len(values) == 0 || values[0] == value
	}
}

func (c *Cluster) String() string {
	if c.value != "" {
		return fmt.Sprintf("cluster:%s", c.value)
	} else {
		return "cluster"
	}
}

type GroupBy struct {
	groups int
}

func NewGroupByConstraint(groups int) *GroupBy {
	return &GroupBy{
		groups: groups,
	}
}

func (g *GroupBy) Matches(value string, values []string) bool {
	counts := make(map[string]int)
	for _, v := range values {
		counts[v] = counts[v] + 1
	}

	if len(counts) < g.groups {
		_, ok := counts[value]
		return !ok
	} else {
		minCount := int(math.MaxInt32)
		for _, v := range counts {
			if v < minCount {
				minCount = v
			}
		}
		if minCount == int(math.MaxInt32) {
			minCount = 0
		}

		return counts[value] == minCount
	}
}

func (g *GroupBy) String() string {
	if g.groups > 1 {
		return fmt.Sprintf("groupBy:%d", g.groups)
	} else {
		return "groupBy"
	}
}

func CheckConstraints(offer *mesos.Offer, taskConstraints map[string][]Constraint, otherTasks []Constrained) string {
	offerAttributes := OfferAttributes(offer)

	for name, constraints := range taskConstraints {
		for _, constraint := range constraints {
			attribute, exists := offerAttributes[name]
			if exists {
				if !constraint.Matches(attribute, otherTasksAttributes(name, otherTasks)) {
					fmt.Printf("Attribute %s doesn't match %s\n", name, constraint) //TODO probably should add logger to this utils?
					return fmt.Sprintf("%s doesn't match %s", name, constraint)
				}
			} else {
				fmt.Printf("Offer does not contain %s attribute\n", name) //TODO and this
				return fmt.Sprintf("no %s", name)
			}
		}
	}

	return ""
}

func OfferAttributes(offer *mesos.Offer) map[string]string {
	offerAttributes := map[string]string{
		"hostname": offer.GetHostname(),
	}

	for _, attribute := range offer.GetAttributes() {
		text := attribute.GetText().GetValue()
		if text != "" {
			offerAttributes[attribute.GetName()] = text
		}
	}

	return offerAttributes
}

func otherTasksAttributes(name string, otherTasks []Constrained) []string {
	attributes := make([]string, 0)
	for _, task := range otherTasks {
		attribute := task.Attribute(name)
		if attribute != "" {
			attributes = append(attributes, attribute)
		}
	}

	return attributes
}

type Constrained interface {
	Constraints() map[string][]Constraint
	Attribute(name string) string
}
