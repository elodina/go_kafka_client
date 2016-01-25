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
	"reflect"
	"testing"
)

func TestRangeInit(t *testing.T) {
	rng, err := ParseRange("30")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if rng == nil {
		t.Fatal("Range should not be nil")
	}

	rng, err = ParseRange("30..31")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if rng == nil {
		t.Fatal("Range should not be nil")
	}

	rng = NewRange(30, 31)
	if rng == nil {
		t.Fatal("Range should not be nil")
	}

	//empty
	rng, err = ParseRange("")
	if err == nil {
		t.Fatalf("Unexpected range: %s", rng)
	}

	// not int
	rng, err = ParseRange("abc")
	if err == nil {
		t.Fatalf("Unexpected range: %s", rng)
	}

	// not int first
	rng, err = ParseRange("abc..31")
	if err == nil {
		t.Fatalf("Unexpected range: %s", rng)
	}

	// not int 2nd
	rng, err = ParseRange("31..abc")
	if err == nil {
		t.Fatalf("Unexpected range: %s", rng)
	}

	// inverted range
	rng, err = ParseRange("10..0")
	if err == nil {
		t.Fatalf("Unexpected range: %s", rng)
	}
}

func TestStartEndRange(t *testing.T) {
	rng, err := ParseRange("0")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if rng.Start() != 0 {
		t.Errorf("Range start should be 0; actual %d", rng.Start())
	}

	rng, err = ParseRange("0..10")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if rng.Start() != 0 {
		t.Errorf("Range start should be 0; actual %d", rng.Start())
	}
	if rng.End() != 10 {
		t.Errorf("Range end should be 10; actual %d", rng.End())
	}
}

func TestRangeOverlap(t *testing.T) {
	// no overlap
	rng := NewRange(0, 10).Overlap(NewRange(20, 30))
	if rng != nil {
		t.Errorf("Range should be nil; actual %s", rng)
	}

	rng = NewRange(20, 30).Overlap(NewRange(0, 10))
	if rng != nil {
		t.Errorf("Range should be nil; actual %s", rng)
	}

	rng = NewRange(0, 0).Overlap(NewRange(1, 1))
	if rng != nil {
		t.Errorf("Range should be nil; actual %s", rng)
	}

	// partial
	rng = NewRange(0, 10).Overlap(NewRange(5, 15))
	if rng.Start() != 5 || rng.End() != 10 {
		t.Errorf("Range should be 5..10; actual %s", rng)
	}

	rng = NewRange(5, 15).Overlap(NewRange(0, 10))
	if rng.Start() != 5 || rng.End() != 10 {
		t.Errorf("Range should be 5..10; actual %s", rng)
	}

	// includes
	rng = NewRange(0, 10).Overlap(NewRange(2, 3))
	if rng.Start() != 2 || rng.End() != 3 {
		t.Errorf("Range should be 2..3; actual %s", rng)
	}

	rng = NewRange(2, 3).Overlap(NewRange(0, 10))
	if rng.Start() != 2 || rng.End() != 3 {
		t.Errorf("Range should be 2..3; actual %s", rng)
	}

	rng = NewRange(0, 10).Overlap(NewRange(5, 5))
	if rng.Start() != 5 || rng.End() != 5 {
		t.Errorf("Range should be 5; actual %s", rng)
	}

	// last point
	rng = NewRange(0, 10).Overlap(NewRange(0, 0))
	if rng.Start() != 0 || rng.End() != 0 {
		t.Errorf("Range should be 0; actual %s", rng)
	}

	rng = NewRange(0, 10).Overlap(NewRange(10, 10))
	if rng.Start() != 10 || rng.End() != 10 {
		t.Errorf("Range should be 10; actual %s", rng)
	}

	rng = NewRange(0, 0).Overlap(NewRange(0, 0))
	if rng.Start() != 0 || rng.End() != 0 {
		t.Errorf("Range should be 0; actual %s", rng)
	}
}

func TestRangeValues(t *testing.T) {
	if !reflect.DeepEqual(NewRange(3, 3).Values(), []int{3}) {
		t.Errorf("NewRange(3, 3).Values() should be []int{3}")
	}

	if !reflect.DeepEqual(NewRange(0, 1).Values(), []int{0, 1}) {
		t.Errorf("NewRange(0, 1).Values() should be []int{0, 1}")
	}

	if !reflect.DeepEqual(NewRange(0, 4).Values(), []int{0, 1, 2, 3, 4}) {
		t.Errorf("NewRange(0, 4).Values() should be []int{0, 1, 2, 3, 4}")
	}
}

func TestRangeString(t *testing.T) {
	if NewRange(0, 0).String() != "0" {
		t.Errorf("NewRange(0, 0).String() should be 0")
	}

	if NewRange(0, 10).String() != "0..10" {
		t.Errorf("NewRange(0, 10).String() should be 0..10")
	}

	rng, err := ParseRange("0..0")
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if rng.String() != "0" {
		t.Errorf("ParseRange(0..0).String() should be 0")
	}
}
