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

package frequencies

const (
	lgMinMapSize = 3
	// This constant is large enough so that computing the median of SAMPLE_SIZE
	// randomly selected entries from a list of numbers and outputting
	// the empirical median will give a constant-factor approximation to the
	// true median with high probability.
	sampleSize = 1024
)

// hash returns an index into the hash table.
// This hash function is taken from the internals of Austin Appleby's MurmurHash3 algorithm.
// It is also used by the Trove for Java libraries.
func hash(okey int64) int64 {
	key := uint64(okey)
	key ^= key >> 33
	key *= 0xff51afd7ed558ccd
	key ^= key >> 33
	key *= 0xc4ceb9fe1a85ec53
	key ^= key >> 33
	return int64(key)
}
