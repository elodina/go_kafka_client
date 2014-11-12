/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import ("fmt"
	"time")

type A struct {
	ch *time.Timer
}

func main() {
	a := &A{}
	a.ch = time.NewTimer(3 * time.Second)
	go a.Start()

	go func() {
		for i := 0; i < 3; i++ {
			time.Sleep(1 * time.Second)
			fmt.Println("replacing")
			a.ch.Reset(3 * time.Second)
		}
	}()

	time.Sleep(20 * time.Second)
}

func (a *A) Start() {
	<-a.ch.C
	fmt.Println("got a value")
}
