/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kll

const (
	_SERIAL_VERSION_EMPTY_FULL = 1 // Empty or full preamble, NOT single item format, NOT updatable
	_SERIAL_VERSION_UPDATABLE  = 3 // PreInts=5, Full preamble + LevelsArr + min, max + empty space
	_PREAMBLE_INTS_FULL        = 5 // Full preamble, not empty nor single item.
)
