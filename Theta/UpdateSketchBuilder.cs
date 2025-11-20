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
using static Apache.DataSketches.Common.Util;

namespace Apache.DataSketches.Theta
{
    /// <summary>
    /// For building a new UpdateSketch.
    /// </summary>
    /// <remarks>
    /// Author: Lee Rhodes
    /// </remarks>
    public class UpdateSketchBuilder
    {
        private int _lgNomLongs;
        private long _seed;
        private ResizeFactor _rf;
        private Family _fam;
        private float _p;

        // Fields for concurrent theta sketch - not implemented in C# version
        // private int _numPoolThreads;
        // private int _localLgNomLongs;
        // private bool _propagateOrderedCompact;
        // private double _maxConcurrencyError;
        // private int _maxNumLocalThreads;

        /// <summary>
        /// Constructor for building a new UpdateSketch. The default configuration is:
        /// <list type="bullet">
        /// <item>Nominal Entries: 4096</item>
        /// <item>Seed: 9001 (DEFAULT_UPDATE_SEED)</item>
        /// <item>Input Sampling Probability: 1.0</item>
        /// <item>Family: QUICKSELECT</item>
        /// <item>Resize Factor: The default for sketches on the heap is X8.
        /// For direct sketches, which are targeted for native memory off the heap, this value will
        /// be fixed at either X1 or X2.</item>
        /// </list>
        /// Parameters unique to the concurrent sketches are not supported in this C# implementation.
        /// </summary>
        public UpdateSketchBuilder()
        {
            _lgNomLongs = BitOperations.TrailingZeroCount((uint)ThetaUtil.DEFAULT_NOMINAL_ENTRIES);
            _seed = ThetaUtil.DEFAULT_UPDATE_SEED;
            _p = 1.0f;
            _rf = ResizeFactor.X8;
            _fam = Family.QUICKSELECT;
            // Default values for concurrent sketch - not implemented
            // _numPoolThreads = 3;
            // _localLgNomLongs = 4; // default is smallest legal QS sketch
            // _propagateOrderedCompact = true;
            // _maxConcurrencyError = 0;
            // _maxNumLocalThreads = 1;
        }

        /// <summary>
        /// Sets the Nominal Entries for this sketch.
        /// This value is also used for building a shared concurrent sketch.
        /// The minimum value is 16 (2^4) and the maximum value is 67,108,864 (2^26).
        /// Be aware that sketches as large as this maximum value may not have been
        /// thoroughly tested or characterized for performance.
        /// </summary>
        /// <param name="nomEntries">Nominal Entries. This will become the ceiling power of 2 if the given value is not.</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetNominalEntries(int nomEntries)
        {
            _lgNomLongs = ThetaUtil.CheckNomLongs(nomEntries);
            return this;
        }

        /// <summary>
        /// Alternative method of setting the Nominal Entries for this sketch from the log_base2 value.
        /// This value is also used for building a shared concurrent sketch.
        /// The minimum value is 4 and the maximum value is 26.
        /// Be aware that sketches as large as this maximum value may not have been
        /// thoroughly characterized for performance.
        /// </summary>
        /// <param name="lgNomEntries">The Log Nominal Entries. Also for the concurrent shared sketch</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetLogNominalEntries(int lgNomEntries)
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
        /// Sets the Nominal Entries for the concurrent local sketch. The minimum value is 16 and the
        /// maximum value is 67,108,864, which is 2^26.
        /// Be aware that sketches as large as this maximum
        /// value have not been thoroughly tested or characterized for performance.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="nomEntries">Nominal Entries. This will become the ceiling power of 2 if it is not.</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetLocalNominalEntries(int nomEntries)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Alternative method of setting the Nominal Entries for a local concurrent sketch from the
        /// log_base2 value.
        /// The minimum value is 4 and the maximum value is 26.
        /// Be aware that sketches as large as this maximum
        /// value have not been thoroughly tested or characterized for performance.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="lgNomEntries">The Log Nominal Entries for a concurrent local sketch</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetLocalLogNominalEntries(int lgNomEntries)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Returns Log-base 2 Nominal Entries for the concurrent local sketch.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <returns>Log-base 2 Nominal Entries for the concurrent local sketch</returns>
        public int GetLocalLgNominalEntries()
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Sets the long seed value that is required by the hashing function.
        /// </summary>
        /// <param name="seed">The seed value</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetSeed(long seed)
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
        /// Sets the upfront uniform sampling probability, p
        /// </summary>
        /// <param name="p">The sampling probability. Must be &gt; 0 and &lt;= 1.0</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetP(float p)
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
        /// Sets the cache Resize Factor.
        /// </summary>
        /// <param name="rf">The Resize Factor</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetResizeFactor(ResizeFactor rf)
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
        /// Set the Family.
        /// </summary>
        /// <param name="family">The family for this builder</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetFamily(Family family)
        {
            _fam = family;
            return this;
        }

        /// <summary>
        /// Returns the Family
        /// </summary>
        /// <returns>The Family</returns>
        public Family GetFamily()
        {
            return _fam;
        }

        /// <summary>
        /// Set the MemoryRequestServer.
        /// Note: Memory management is not implemented in this C# version.
        /// </summary>
        /// <param name="memReqSvr">The given MemoryRequestServer</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetMemoryRequestServer(object memReqSvr)
        {
            // Memory request server not implemented in C# version
            return this;
        }

        /// <summary>
        /// Returns the MemoryRequestServer.
        /// Note: Memory management is not implemented in this C# version.
        /// </summary>
        /// <returns>The MemoryRequestServer (always null in C# version)</returns>
        public object GetMemoryRequestServer()
        {
            return null;
        }

        /// <summary>
        /// Sets the number of pool threads used for background propagation in the concurrent sketches.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="numPoolThreads">The given number of pool threads</param>
        public void SetNumPoolThreads(int numPoolThreads)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Gets the number of background pool threads used for propagation in the concurrent sketches.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <returns>The number of background pool threads</returns>
        public int GetNumPoolThreads()
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Sets the Propagate Ordered Compact flag to the given value. Used with concurrent sketches.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="prop">The given value</param>
        /// <returns>This UpdateSketchBuilder</returns>
        public UpdateSketchBuilder SetPropagateOrderedCompact(bool prop)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Gets the Propagate Ordered Compact flag used with concurrent sketches.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <returns>The Propagate Ordered Compact flag</returns>
        public bool GetPropagateOrderedCompact()
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Sets the Maximum Concurrency Error.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="maxConcurrencyError">The given Maximum Concurrency Error.</param>
        public void SetMaxConcurrencyError(double maxConcurrencyError)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Gets the Maximum Concurrency Error.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <returns>The Maximum Concurrency Error</returns>
        public double GetMaxConcurrencyError()
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Sets the Maximum Number of Local Threads.
        /// This is used to set the size of the local concurrent buffers.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="maxNumLocalThreads">The given Maximum Number of Local Threads</param>
        public void SetMaxNumLocalThreads(int maxNumLocalThreads)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Gets the Maximum Number of Local Threads.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <returns>The Maximum Number of Local Threads.</returns>
        public int GetMaxNumLocalThreads()
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        // BUILD FUNCTIONS

        /// <summary>
        /// Returns an UpdateSketch with the current configuration of this Builder.
        /// </summary>
        /// <returns>An UpdateSketch</returns>
        public UpdateSketch Build()
        {
            return Build(null);
        }

        /// <summary>
        /// Returns an UpdateSketch with the current configuration of this Builder
        /// with the specified backing destination Memory store.
        /// Note: this cannot be used with the Alpha Family of sketches.
        /// Note: Direct sketches are not yet supported in this C# implementation.
        /// </summary>
        /// <param name="dstMem">The destination Memory.</param>
        /// <returns>An UpdateSketch</returns>
        public UpdateSketch Build(Memory<byte>? dstMem)
        {
            UpdateSketch sketch;
            switch (_fam)
            {
                case Family.ALPHA:
                    if (dstMem == null)
                    {
                        throw new SketchesArgumentException(
                            "AlphaSketch is not supported in this C# implementation");
                    }
                    else
                    {
                        throw new SketchesArgumentException("AlphaSketch cannot be made Direct to Memory.");
                    }

                case Family.QUICKSELECT:
                    if (dstMem == null)
                    {
                        sketch = new HeapQuickSelectSketch(_lgNomLongs, _seed, _p, _rf, false);
                    }
                    else
                    {
                        throw new SketchesArgumentException(
                            "Direct sketches are not yet supported in this C# implementation");
                    }
                    break;

                default:
                    throw new SketchesArgumentException(
                        $"Given Family cannot be built as a Theta Sketch: {_fam}");
            }
            return sketch;
        }

        /// <summary>
        /// Returns an on-heap concurrent shared UpdateSketch with the current configuration of the
        /// Builder.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <returns>An on-heap concurrent UpdateSketch with the current configuration of the Builder.</returns>
        public UpdateSketch BuildShared()
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Returns a direct (potentially off-heap) concurrent shared UpdateSketch with the current
        /// configuration of the Builder and the given destination Memory. If the destination
        /// Memory is null, this defaults to an on-heap concurrent shared UpdateSketch.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="dstMem">The given Memory for Direct, otherwise null.</param>
        /// <returns>A concurrent UpdateSketch with the current configuration of the Builder
        /// and the given destination Memory.</returns>
        public UpdateSketch BuildShared(Memory<byte>? dstMem)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Returns a direct (potentially off-heap) concurrent shared UpdateSketch with the current
        /// configuration of the Builder, the data from the given sketch, and the given destination
        /// Memory. If the destination Memory is null, this defaults to an on-heap
        /// concurrent shared UpdateSketch.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="sketch">A given UpdateSketch from which the data is used to initialize the returned
        /// shared sketch.</param>
        /// <param name="dstMem">The given Memory for Direct, otherwise null.</param>
        /// <returns>A concurrent UpdateSketch with the current configuration of the Builder
        /// and the given destination Memory.</returns>
        public UpdateSketch BuildSharedFromSketch(UpdateSketch sketch, Memory<byte>? dstMem)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Returns a local, on-heap, concurrent UpdateSketch to be used as a per-thread local buffer
        /// along with the given concurrent shared UpdateSketch and the current configuration of this
        /// Builder.
        /// Note: Concurrent sketches are not implemented in this C# version.
        /// </summary>
        /// <param name="shared">The concurrent shared sketch to be accessed via the concurrent local sketch.</param>
        /// <returns>An UpdateSketch to be used as a per-thread local buffer.</returns>
        public UpdateSketch BuildLocal(UpdateSketch shared)
        {
            throw new SketchesArgumentException(
                "Concurrent sketches are not supported in this C# implementation");
        }

        /// <summary>
        /// Returns a string representation of this UpdateSketchBuilder configuration
        /// </summary>
        /// <returns>A string representation of this UpdateSketchBuilder configuration</returns>
        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.AppendLine("UpdateSketchBuilder configuration:");
            sb.Append("LgK:").Append(TAB).Append(_lgNomLongs).Append(LS);
            sb.Append("K:").Append(TAB).Append(1 << _lgNomLongs).Append(LS);
            sb.Append("Seed:").Append(TAB).Append(_seed).Append(LS);
            sb.Append("p:").Append(TAB).Append(_p).Append(LS);
            sb.Append("ResizeFactor:").Append(TAB).Append(_rf).Append(LS);
            sb.Append("Family:").Append(TAB).Append(_fam).Append(LS);
            return sb.ToString();
        }
    }
}
