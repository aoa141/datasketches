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

using System;
using System.Numerics;
using System.Text;
using Apache.DataSketches.Common;
using Apache.DataSketches.ThetaCommon;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// For building a new SetOperation.
    /// </summary>
    public class SetOperationBuilder
    {
        private int _lgNomLongs;
        private long _seed;
        private ResizeFactor _rf;
        private float _p;

        /// <summary>
        /// Constructor for building a new SetOperation. The default configuration is:
        /// - Max Nominal Entries (max K): 4096
        /// - Seed: 9001 (DEFAULT_UPDATE_SEED)
        /// - ResizeFactor: X8
        /// - Input Sampling Probability: 1.0
        /// - Memory: null
        /// </summary>
        public SetOperationBuilder()
        {
            _lgNomLongs = BitOperations.TrailingZeroCount((uint)ThetaUtil.DEFAULT_NOMINAL_ENTRIES);
            _seed = ThetaUtil.DEFAULT_UPDATE_SEED;
            _p = 1.0f;
            _rf = ResizeFactor.X8;
        }

        /// <summary>
        /// Sets the Maximum Nominal Entries (max K) for this set operation. The effective value of K of the result of a
        /// Set Operation can be less than max K, but never greater.
        /// The minimum value is 16 and the maximum value is 67,108,864, which is 2^26.
        /// </summary>
        /// <param name="nomEntries">Nominal Entries. This will become the ceiling power of 2 if it is not a power of 2.</param>
        /// <returns>This SetOperationBuilder</returns>
        public SetOperationBuilder SetNominalEntries(int nomEntries)
        {
            _lgNomLongs = BitOperations.TrailingZeroCount((uint)Util.CeilingPowerOf2(nomEntries));
            if (_lgNomLongs > ThetaUtil.MAX_LG_NOM_LONGS || _lgNomLongs < ThetaUtil.MIN_LG_NOM_LONGS)
            {
                throw new SketchesArgumentException(
                    $"Nominal Entries must be >= 16 and <= 67108864: {nomEntries}");
            }
            return this;
        }

        /// <summary>
        /// Alternative method of setting the Nominal Entries for this set operation from the log_base2 value.
        /// The minimum value is 4 and the maximum value is 26.
        /// Be aware that set operations as large as this maximum value may not have been thoroughly characterized for performance.
        /// </summary>
        /// <param name="lgNomEntries">The log_base2 Nominal Entries.</param>
        /// <returns>This SetOperationBuilder</returns>
        public SetOperationBuilder SetLogNominalEntries(int lgNomEntries)
        {
            _lgNomLongs = ThetaUtil.CheckNomLongs(1 << lgNomEntries);
            return this;
        }

        /// <summary>
        /// Returns Log-base 2 Nominal Entries
        /// </summary>
        /// <returns>Log-base 2 Nominal Entries</returns>
        public int GetLgNominalEntries()
        {
            return _lgNomLongs;
        }

        /// <summary>
        /// Sets the long seed value that is required by the hashing function.
        /// </summary>
        /// <param name="seed">The seed value</param>
        /// <returns>This SetOperationBuilder</returns>
        public SetOperationBuilder SetSeed(long seed)
        {
            _seed = seed;
            return this;
        }

        /// <summary>
        /// Returns the seed
        /// </summary>
        /// <returns>The seed</returns>
        public long GetSeed()
        {
            return _seed;
        }

        /// <summary>
        /// Sets the upfront uniform sampling probability, p. Although this functionality is
        /// implemented for Unions only, it rarely makes sense to use it. The proper use of upfront
        /// sampling is when building the sketches.
        /// </summary>
        /// <param name="p">The sampling probability. Must be &gt; 0 and &lt;= 1.0</param>
        /// <returns>This SetOperationBuilder</returns>
        public SetOperationBuilder SetP(float p)
        {
            if (p <= 0.0 || p > 1.0)
            {
                throw new SketchesArgumentException($"p must be > 0 and <= 1.0: {p}");
            }
            _p = p;
            return this;
        }

        /// <summary>
        /// Returns the pre-sampling probability p
        /// </summary>
        /// <returns>The pre-sampling probability p</returns>
        public float GetP()
        {
            return _p;
        }

        /// <summary>
        /// Sets the cache Resize Factor
        /// </summary>
        /// <param name="rf">The Resize Factor</param>
        /// <returns>This SetOperationBuilder</returns>
        public SetOperationBuilder SetResizeFactor(ResizeFactor rf)
        {
            _rf = rf;
            return this;
        }

        /// <summary>
        /// Returns the Resize Factor
        /// </summary>
        /// <returns>The Resize Factor</returns>
        public ResizeFactor GetResizeFactor()
        {
            return _rf;
        }

        /// <summary>
        /// Returns a SetOperation with the current configuration of this Builder and the given Family.
        /// </summary>
        /// <param name="family">The chosen SetOperation family</param>
        /// <returns>A SetOperation</returns>
        public SetOperation Build(Family family)
        {
            return Build(family, null);
        }

        /// <summary>
        /// Returns a SetOperation with the current configuration of this Builder, the given Family
        /// and the given destination memory. Note that the destination memory cannot be used with AnotB.
        /// </summary>
        /// <param name="family">The chosen SetOperation family</param>
        /// <param name="dstMem">The destination Memory.</param>
        /// <returns>A SetOperation</returns>
        public SetOperation Build(Family family, Memory<byte>? dstMem)
        {
            SetOperation setOp;
            switch (family)
            {
                case Family.UNION:
                    if (dstMem == null)
                    {
                        setOp = UnionImpl.InitNewHeapInstance(_lgNomLongs, _seed, _p, _rf);
                    }
                    else
                    {
                        setOp = UnionImpl.InitNewDirectInstance(_lgNomLongs, _seed, _p, _rf, dstMem.Value);
                    }
                    break;

                case Family.INTERSECTION:
                    if (dstMem == null)
                    {
                        setOp = IntersectionImpl.InitNewHeapInstance(_seed);
                    }
                    else
                    {
                        setOp = IntersectionImpl.InitNewDirectInstance(_seed, dstMem.Value);
                    }
                    break;

                case Family.A_NOT_B:
                    if (dstMem == null)
                    {
                        setOp = new AnotBImpl(_seed);
                    }
                    else
                    {
                        throw new SketchesArgumentException("AnotB cannot be persisted.");
                    }
                    break;

                default:
                    throw new SketchesArgumentException(
                        $"Given Family cannot be built as a SetOperation: {family}");
            }
            return setOp;
        }

        /// <summary>
        /// Convenience method, returns a configured SetOperation Union with Default Nominal Entries
        /// </summary>
        /// <returns>A Union object</returns>
        public Union BuildUnion()
        {
            return (Union)Build(Family.UNION);
        }

        /// <summary>
        /// Convenience method, returns a configured SetOperation Union with Default Nominal Entries
        /// and the given destination memory.
        /// </summary>
        /// <param name="dstMem">The destination Memory.</param>
        /// <returns>A Union object</returns>
        public Union BuildUnion(Memory<byte> dstMem)
        {
            return (Union)Build(Family.UNION, dstMem);
        }

        /// <summary>
        /// Convenience method, returns a configured SetOperation Intersection with Default Nominal Entries
        /// </summary>
        /// <returns>An Intersection object</returns>
        public Intersection BuildIntersection()
        {
            return (Intersection)Build(Family.INTERSECTION);
        }

        /// <summary>
        /// Convenience method, returns a configured SetOperation Intersection with Default Nominal Entries
        /// and the given destination memory.
        /// </summary>
        /// <param name="dstMem">The destination Memory.</param>
        /// <returns>An Intersection object</returns>
        public Intersection BuildIntersection(Memory<byte> dstMem)
        {
            return (Intersection)Build(Family.INTERSECTION, dstMem);
        }

        /// <summary>
        /// Convenience method, returns a configured SetOperation ANotB with Default Update Seed
        /// </summary>
        /// <returns>An ANotB object</returns>
        public AnotB BuildANotB()
        {
            return (AnotB)Build(Family.A_NOT_B);
        }

        /// <summary>
        /// Returns a string representation of this SetOperationBuilder configuration
        /// </summary>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("SetOperationBuilder configuration:");
            sb.AppendLine($"LgK:\t{_lgNomLongs}");
            sb.AppendLine($"K:\t{1 << _lgNomLongs}");
            sb.AppendLine($"Seed:\t{_seed}");
            sb.AppendLine($"p:\t{_p}");
            sb.AppendLine($"ResizeFactor:\t{_rf}");
            return sb.ToString();
        }
    }
}
