/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

namespace Apache.DataSketches.Theta
{
    internal class HeapHashIterator : HashIterator
    {
        private readonly long[] _cache;
        private readonly long _thetaLong;
        private int _index;
        private long _hash;

        internal HeapHashIterator(long[] cache, long thetaLong)
        {
            _cache = cache;
            _thetaLong = thetaLong;
            _index = -1;
            _hash = 0;
        }

        public long Get()
        {
            return _hash;
        }

        public bool Next()
        {
            while (++_index < _cache.Length)
            {
                _hash = _cache[_index];
                if (_hash != 0 && _hash < _thetaLong)
                {
                    return true;
                }
            }
            return false;
        }
    }
}
