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
	"fmt"
	"math"
	"strconv"
	"strings"
)

type Range struct {
	start int
	end   int
}

func (r *Range) Start() int {
	return r.start
}

func (r *Range) End() int {
	return r.end
}

func (r *Range) Overlap(r2 *Range) *Range {
	r1 := r
	if r1.start > r2.start {
		tmp := r1
		r1 = r2
		r2 = tmp
	}

	if r2.start > r1.end {
		return nil
	}

	start := r2.start
	end := int(math.Min(float64(r1.end), float64(r2.end)))
	return NewRange(start, end)
}

func (r *Range) Values() []int {
	values := make([]int, (r.end-r.start)+1)
	for i := r.start; i <= r.end; i++ {
		values[i-r.start] = i
	}

	return values
}

func (r *Range) String() string {
	if r.start == r.end {
		return strconv.Itoa(r.start)
	} else {
		return fmt.Sprintf("%d..%d", r.start, r.end)
	}
}

func NewRange(start int, end int) *Range {
	return &Range{
		start: start,
		end:   end,
	}
}

func ParseRange(s string) (*Range, error) {
	idx := strings.Index(s, "..")

	if idx == -1 {
		value, err := strconv.Atoi(s)
		if err != nil {
			return nil, err
		}

		return NewRange(value, value), nil
	} else {
		start, err := strconv.Atoi(s[:idx])
		if err != nil {
			return nil, err
		}
		end, err := strconv.Atoi(s[idx+2:])
		if err != nil {
			return nil, err
		}
		if start > end {
			return nil, fmt.Errorf("Range start > end (%d > %d)", start, end)
		}

		return NewRange(start, end), nil
	}
}

func ParseRanges(rawRanges string, sep string) ([]*Range, error) {
	if rawRanges == "" {
		return nil, nil
	} else {
		splitRanges := strings.Split(rawRanges, sep)
		ranges := make([]*Range, len(splitRanges))
		for idx, rawRange := range splitRanges {
			rng, err := ParseRange(rawRange)
			if err != nil {
				return nil, fmt.Errorf("Unparsable range %s at index %d: %s", rawRange, idx, err)
			}

			ranges[idx] = rng
		}

		return ranges, nil
	}
}
