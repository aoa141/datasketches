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
using System.Collections.Generic;
using System.Numerics;
using System.Text;

namespace Apache.DataSketches.Common
{
    /// <summary>
    /// Common utility functions.
    /// </summary>
    /// <remarks>
    /// Author: Lee Rhodes
    /// </remarks>
    public static class Util
    {
        /// <summary>
        /// The line separator character as a String.
        /// </summary>
        public static readonly string LS = Environment.NewLine;

        /// <summary>
        /// The tab character
        /// </summary>
        public const char TAB = '\t';

        /// <summary>
        /// The natural logarithm of 2.0.
        /// </summary>
        public static readonly double LOG2 = Math.Log(2.0);

        /// <summary>
        /// The inverse golden ratio as an unsigned long.
        /// </summary>
        public const long INVERSE_GOLDEN_U64 = unchecked((long)0x9e3779b97f4a7c13UL);

        /// <summary>
        /// The inverse golden ratio as a fraction.
        /// This has more precision than using the formula: (Math.Sqrt(5.0) - 1.0) / 2.0.
        /// </summary>
        public const double INVERSE_GOLDEN = 0.6180339887498949025;

        /// <summary>
        /// Long.MaxValue as a double.
        /// </summary>
        public static readonly double LONG_MAX_VALUE_AS_DOUBLE = long.MaxValue;

        // Byte Conversions

        /// <summary>
        /// Returns an int extracted from a Little-Endian byte array.
        /// </summary>
        /// <param name="arr">The given byte array</param>
        /// <returns>An int extracted from a Little-Endian byte array.</returns>
        public static int BytesToInt(byte[] arr)
        {
            return arr[3] << 24
                | (arr[2] & 0xff) << 16
                | (arr[1] & 0xff) << 8
                | (arr[0] & 0xff);
        }

        /// <summary>
        /// Returns a long extracted from a Little-Endian byte array.
        /// </summary>
        /// <param name="arr">The given byte array</param>
        /// <returns>A long extracted from a Little-Endian byte array.</returns>
        public static long BytesToLong(byte[] arr)
        {
            return (long)arr[7] << 56
                | ((long)arr[6] & 0xff) << 48
                | ((long)arr[5] & 0xff) << 40
                | ((long)arr[4] & 0xff) << 32
                | ((long)arr[3] & 0xff) << 24
                | ((long)arr[2] & 0xff) << 16
                | ((long)arr[1] & 0xff) << 8
                | ((long)arr[0] & 0xff);
        }

        /// <summary>
        /// Returns a Little-Endian byte array extracted from the given int.
        /// </summary>
        /// <param name="v">The given int</param>
        /// <param name="arr">A given array of 4 bytes that will be returned with the data</param>
        /// <returns>A Little-Endian byte array extracted from the given int.</returns>
        public static byte[] IntToBytes(int v, byte[] arr)
        {
            arr[3] = (byte)(v >> 24);
            arr[2] = (byte)(v >> 16);
            arr[1] = (byte)(v >> 8);
            arr[0] = (byte)v;
            return arr;
        }

        /// <summary>
        /// Returns a Little-Endian byte array extracted from the given long.
        /// </summary>
        /// <param name="v">The given long</param>
        /// <param name="arr">A given array of 8 bytes that will be returned with the data</param>
        /// <returns>A Little-Endian byte array extracted from the given long.</returns>
        public static byte[] LongToBytes(long v, byte[] arr)
        {
            arr[7] = (byte)(v >> 56);
            arr[6] = (byte)(v >> 48);
            arr[5] = (byte)(v >> 40);
            arr[4] = (byte)(v >> 32);
            arr[3] = (byte)(v >> 24);
            arr[2] = (byte)(v >> 16);
            arr[1] = (byte)(v >> 8);
            arr[0] = (byte)v;
            return arr;
        }

        // Byte array conversions

        internal static long[] ConvertToLongArray(byte[] byteArr, bool littleEndian)
        {
            int len = byteArr.Length;
            long[] longArr = new long[len / 8 + (len % 8 != 0 ? 1 : 0)];
            int off = 0;
            int longArrIdx = 0;
            while (off < len)
            {
                int rem = Math.Min(len - 1 - off, 7);
                long tgt = 0;
                if (littleEndian)
                {
                    for (int j = off + rem, k = 0; j >= off; --j, k++)
                    {
                        tgt |= ((long)byteArr[j] & 0XFFL) << (k * 8);
                    }
                }
                else // BE
                {
                    for (int j = off + rem, k = rem; j >= off; --j, k--)
                    {
                        tgt |= ((long)byteArr[j] & 0XFFL) << (k * 8);
                    }
                }
                off += 8;
                longArr[longArrIdx++] = tgt;
            }
            return longArr;
        }

        // String Related

        /// <summary>
        /// Returns a string of spaced hex bytes in Big-Endian order.
        /// </summary>
        /// <param name="v">The given long</param>
        /// <returns>String of spaced hex bytes in Big-Endian order.</returns>
        public static string LongToHexBytes(long v)
        {
            const long mask = 0XFFL;
            StringBuilder sb = new StringBuilder();
            for (int i = 8; i-- > 0;)
            {
                string s = ((v >> (i * 8)) & mask).ToString("x");
                sb.Append(ZeroPad(s, 2)).Append(" ");
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns a string view of a byte array
        /// </summary>
        /// <param name="arr">The given byte array</param>
        /// <param name="signed">Set true if you want the byte values signed.</param>
        /// <param name="littleEndian">Set true if you want Little-Endian order</param>
        /// <param name="sep">The separator string between bytes</param>
        /// <returns>A string view of a byte array</returns>
        public static string BytesToString(byte[] arr, bool signed, bool littleEndian, string sep)
        {
            StringBuilder sb = new StringBuilder();
            int mask = signed ? unchecked((int)0XFFFFFFFF) : 0XFF;
            int arrLen = arr.Length;
            if (littleEndian)
            {
                for (int i = 0; i < arrLen - 1; i++)
                {
                    sb.Append(arr[i] & mask).Append(sep);
                }
                sb.Append(arr[arrLen - 1] & mask);
            }
            else
            {
                for (int i = arrLen; i-- > 1;)
                {
                    sb.Append(arr[i] & mask).Append(sep);
                }
                sb.Append(arr[0] & mask);
            }
            return sb.ToString();
        }

        /// <summary>
        /// Returns the given time in nanoseconds formatted as Sec.mSec_uSec_nSec
        /// </summary>
        /// <param name="nS">The given nanoseconds</param>
        /// <returns>The given time in nanoseconds formatted as Sec.mSec_uSec_nSec</returns>
        public static string NanoSecToString(long nS)
        {
            long rem_nS = (long)(nS % 1000.0);
            long rem_uS = (long)(nS / 1000.0 % 1000.0);
            long rem_mS = (long)(nS / 1000000.0 % 1000.0);
            long sec = (long)(nS / 1000000000.0);
            string nSstr = ZeroPad(rem_nS.ToString(), 3);
            string uSstr = ZeroPad(rem_uS.ToString(), 3);
            string mSstr = ZeroPad(rem_mS.ToString(), 3);
            return $"{sec}.{mSstr}_{uSstr}_{nSstr}";
        }

        /// <summary>
        /// Returns the given time in milliseconds formatted as Hours:Min:Sec.mSec
        /// </summary>
        /// <param name="mS">The given milliseconds</param>
        /// <returns>The given time in milliseconds formatted as Hours:Min:Sec.mSec</returns>
        public static string MilliSecToString(long mS)
        {
            long rem_mS = (long)(mS % 1000.0);
            long rem_sec = (long)(mS / 1000.0 % 60.0);
            long rem_min = (long)(mS / 60000.0 % 60.0);
            long hr = (long)(mS / 3600000.0);
            string mSstr = ZeroPad(rem_mS.ToString(), 3);
            string secStr = ZeroPad(rem_sec.ToString(), 2);
            string minStr = ZeroPad(rem_min.ToString(), 2);
            return $"{hr}:{minStr}:{secStr}.{mSstr}";
        }

        /// <summary>
        /// Prepend the given string with zeros. If the given string is equal or greater than the given
        /// field length, it will be returned without modification.
        /// </summary>
        /// <param name="s">The given string</param>
        /// <param name="fieldLength">Desired total field length including the given string</param>
        /// <returns>The given string prepended with zeros.</returns>
        public static string ZeroPad(string s, int fieldLength)
        {
            return CharacterPad(s, fieldLength, '0', false);
        }

        /// <summary>
        /// Prepend or postpend the given string with the given character to fill the given field length.
        /// If the given string is equal to or greater than the given field length, it will be returned
        /// without modification.
        /// </summary>
        /// <param name="s">The given string</param>
        /// <param name="fieldLength">The desired field length</param>
        /// <param name="padChar">The desired pad character</param>
        /// <param name="postpend">If true append the padCharacters to the end of the string.</param>
        /// <returns>Prepended or postpended given string with the given character to fill the given field length.</returns>
        public static string CharacterPad(string s, int fieldLength, char padChar, bool postpend)
        {
            int sLen = s.Length;
            if (sLen < fieldLength)
            {
                char[] cArr = new char[fieldLength - sLen];
                Array.Fill(cArr, padChar);
                string addstr = new string(cArr);
                return postpend ? s + addstr : addstr + s;
            }
            return s;
        }

        // Memory byte alignment

        /// <summary>
        /// Checks if parameter v is a multiple of 8 and greater than zero.
        /// </summary>
        /// <param name="v">The parameter to check</param>
        /// <param name="argName">This name will be part of the error message if the check fails.</param>
        public static void CheckIfMultipleOf8AndGT0(long v, string argName)
        {
            if ((v & 0X7L) == 0L && v > 0L)
            {
                return;
            }
            throw new SketchesArgumentException($"The value of the parameter \"{argName}\" " +
                $"must be a positive multiple of 8 and greater than zero: {v}");
        }

        /// <summary>
        /// Returns true if v is a multiple of 8 and greater than zero
        /// </summary>
        /// <param name="v">The parameter to check</param>
        /// <returns>True if v is a multiple of 8 and greater than zero</returns>
        public static bool IsMultipleOf8AndGT0(long v)
        {
            return (v & 0X7L) == 0L && v > 0L;
        }

        // Powers of 2 or powers of base related

        /// <summary>
        /// Returns true if given long argument is exactly a positive power of 2.
        /// </summary>
        /// <param name="n">The input argument.</param>
        /// <returns>True if argument is exactly a positive power of 2.</returns>
        public static bool IsPowerOf2(long n)
        {
            return (n > 0) && ((n & (n - 1L)) == 0);
        }

        /// <summary>
        /// Checks the given long argument if it is a positive integer power of 2.
        /// If not, it throws an exception with the user supplied local argument name, if not null.
        /// </summary>
        /// <param name="n">The input long argument must be a positive integer power of 2.</param>
        /// <param name="argName">Used in the thrown exception. It may be null.</param>
        /// <exception cref="SketchesArgumentException">If not a positive integer power of 2.</exception>
        public static void CheckIfPowerOf2(long n, string argName)
        {
            if (IsPowerOf2(n)) { return; }
            argName = argName ?? "";
            throw new SketchesArgumentException($"The value of the argument \"{argName}\" " +
                $"must be a positive integer power of 2: {n}");
        }

        /// <summary>
        /// Computes the int ceiling power of 2 within the range [1, 2^30].
        /// </summary>
        /// <param name="n">The input int argument.</param>
        /// <returns>The ceiling power of 2.</returns>
        public static int CeilingPowerOf2(int n)
        {
            if (n <= 1) { return 1; }
            const int topIntPwrOf2 = 1 << 30;
            return n >= topIntPwrOf2 ? topIntPwrOf2 : (int)BitOperations.RoundUpToPowerOf2((uint)n);
        }

        /// <summary>
        /// Computes the long ceiling power of 2 within the range [1, 2^62].
        /// </summary>
        /// <param name="n">The input long argument.</param>
        /// <returns>The ceiling power of 2.</returns>
        public static long CeilingPowerOf2(long n)
        {
            if (n <= 1L) { return 1L; }
            const long topLongPwrOf2 = 1L << 62;
            if (n >= topLongPwrOf2) { return topLongPwrOf2; }

            // For values that fit in uint range
            if (n <= uint.MaxValue)
            {
                return BitOperations.RoundUpToPowerOf2((uint)n);
            }

            // For larger values, use bit manipulation
            int shift = 64 - BitOperations.LeadingZeroCount((ulong)(n - 1));
            return 1L << shift;
        }

        /// <summary>
        /// Computes the floor power of 2 given n is in the range [1, 2^31-1].
        /// </summary>
        /// <param name="n">The given int argument.</param>
        /// <returns>The floor power of 2 as an int.</returns>
        public static int FloorPowerOf2(int n)
        {
            if (n <= 1) { return 1; }
            return 1 << (31 - BitOperations.LeadingZeroCount((uint)n));
        }

        /// <summary>
        /// Computes the floor power of 2 given n is in the range [1, 2^63-1].
        /// </summary>
        /// <param name="n">The given long argument.</param>
        /// <returns>The floor power of 2 as a long</returns>
        public static long FloorPowerOf2(long n)
        {
            if (n <= 1) { return 1; }
            return 1L << (63 - BitOperations.LeadingZeroCount((ulong)n));
        }

        /// <summary>
        /// Computes the inverse integer power of 2: 1/(2^e) = 2^(-e).
        /// </summary>
        /// <param name="e">A positive value between 0 and 1023 inclusive</param>
        /// <returns>The inverse integer power of 2: 1/(2^e) = 2^(-e)</returns>
        public static double InvPow2(int e)
        {
            return BitConverter.Int64BitsToDouble((1023L - e) << 52);
        }

        /// <summary>
        /// Computes the next larger integer point in the power series point = 2^(i / ppo)
        /// </summary>
        /// <param name="ppo">Points-Per-Octave, or the number of points per integer powers of 2 in the series.</param>
        /// <param name="curPoint">The current point of the series. Must be &gt;= 1.</param>
        /// <returns>The next point in the power series.</returns>
        public static long Pwr2SeriesNext(int ppo, long curPoint)
        {
            long cur = curPoint < 1L ? 1L : curPoint;
            int gi = (int)Math.Round(Log2(cur) * ppo); // current generating index
            long next;
            do
            {
                next = (long)Math.Round(Math.Pow(2.0, (double)++gi / ppo));
            } while (next <= curPoint);
            return next;
        }

        /// <summary>
        /// Computes the previous, smaller integer point in the power series point = 2^(i / ppo)
        /// </summary>
        /// <param name="ppo">Points-Per-Octave, or the number of points per integer powers of 2 in the series.</param>
        /// <param name="curPoint">The current point of the series. Must be &gt;= 1.</param>
        /// <returns>The previous, smaller point in the power series. A returned value of zero terminates the series.</returns>
        public static int Pwr2SeriesPrev(int ppo, int curPoint)
        {
            if (curPoint <= 1) { return 0; }
            int gi = (int)Math.Round(Log2(curPoint) * ppo); // current generating index
            int prev;
            do
            {
                prev = (int)Math.Round(Math.Pow(2.0, (double)--gi / ppo));
            } while (prev >= curPoint);
            return prev;
        }

        /// <summary>
        /// Computes the next larger double in the power series point = logBase^(i / ppb)
        /// </summary>
        /// <param name="ppb">Points-Per-Base, or the number of points per integer powers of base in the series.</param>
        /// <param name="curPoint">The current point of the series. Must be &gt;= 1.0.</param>
        /// <param name="roundToLong">If true the output will be rounded to the nearest long.</param>
        /// <param name="logBase">The desired base of the logarithms</param>
        /// <returns>The next point in the power series.</returns>
        public static double PowerSeriesNextDouble(int ppb, double curPoint, bool roundToLong, double logBase)
        {
            double cur = curPoint < 1.0 ? 1.0 : curPoint;
            double gi = Math.Round(LogBaseOfX(logBase, cur) * ppb); // current generating index
            double next;
            do
            {
                double n = Math.Pow(logBase, ++gi / ppb);
                next = roundToLong ? Math.Round(n) : n;
            } while (next <= cur);
            return next;
        }

        /// <summary>
        /// Returns the ceiling of a given n given a base, where the ceiling is an integral power of the base.
        /// </summary>
        /// <param name="base_">The number in the expression ceiling(base^n).</param>
        /// <param name="n">The input argument.</param>
        /// <returns>The ceiling power of base as a double and equal to a mathematical integer.</returns>
        public static double CeilingPowerBaseOfDouble(double base_, double n)
        {
            double x = n < 1.0 ? 1.0 : n;
            return Math.Round(Math.Pow(base_, Math.Ceiling(LogBaseOfX(base_, x))));
        }

        /// <summary>
        /// Computes the floor of a given n given base, where the floor is an integral power of the base.
        /// </summary>
        /// <param name="base_">The number in the expression floor(base^n).</param>
        /// <param name="n">The input argument.</param>
        /// <returns>The floor power of 2 and equal to a mathematical integer.</returns>
        public static double FloorPowerBaseOfDouble(double base_, double n)
        {
            double x = n < 1.0 ? 1.0 : n;
            return Math.Round(Math.Pow(base_, Math.Floor(LogBaseOfX(base_, x))));
        }

        // Logarithm related

        /// <summary>
        /// The log2(value)
        /// </summary>
        /// <param name="value">The given value</param>
        /// <returns>log2(value)</returns>
        public static double Log2(double value)
        {
            return Math.Log(value) / LOG2;
        }

        /// <summary>
        /// Returns the log_base(x).
        /// </summary>
        /// <param name="base_">The number in the expression log(x) / log(base).</param>
        /// <param name="x">The given value</param>
        /// <returns>The log_base(x)</returns>
        public static double LogBaseOfX(double base_, double x)
        {
            return Math.Log(x) / Math.Log(base_);
        }

        /// <summary>
        /// Returns the number of one bits following the lowest-order ("rightmost") zero-bit in the
        /// two's complement binary representation of the specified long value, or 64 if the value is equal to minus one.
        /// </summary>
        /// <param name="v">The value whose number of trailing ones is to be computed.</param>
        /// <returns>The number of one bits following the lowest-order ("rightmost") zero-bit.</returns>
        public static int NumberOfTrailingOnes(long v)
        {
            return BitOperations.TrailingZeroCount(~(ulong)v);
        }

        /// <summary>
        /// Returns the number of one bits preceding the highest-order ("leftmost") zero-bit in the
        /// two's complement binary representation of the specified long value, or 64 if the value is equal to minus one.
        /// </summary>
        /// <param name="v">The value whose number of leading ones is to be computed.</param>
        /// <returns>The number of one bits preceding the lowest-order ("rightmost") zero-bit.</returns>
        public static int NumberOfLeadingOnes(long v)
        {
            return BitOperations.LeadingZeroCount(~(ulong)v);
        }

        /// <summary>
        /// Returns the log2 of the given int value if it is an exact power of 2 and greater than zero.
        /// </summary>
        /// <param name="powerOf2">Must be a power of 2 and greater than zero.</param>
        /// <param name="argName">The argument name used in the exception if thrown.</param>
        /// <returns>The log2 of the given value if it is an exact power of 2 and greater than zero.</returns>
        /// <exception cref="SketchesArgumentException">If not a power of 2 nor greater than zero.</exception>
        public static int ExactLog2OfInt(int powerOf2, string argName)
        {
            CheckIfPowerOf2(powerOf2, argName);
            return BitOperations.TrailingZeroCount((uint)powerOf2);
        }

        /// <summary>
        /// Returns the log2 of the given long value if it is an exact power of 2 and greater than zero.
        /// </summary>
        /// <param name="powerOf2">Must be a power of 2 and greater than zero.</param>
        /// <param name="argName">The argument name used in the exception if thrown.</param>
        /// <returns>The log2 of the given value if it is an exact power of 2 and greater than zero.</returns>
        /// <exception cref="SketchesArgumentException">If not a power of 2 nor greater than zero.</exception>
        public static int ExactLog2OfLong(long powerOf2, string argName)
        {
            CheckIfPowerOf2(powerOf2, argName);
            return BitOperations.TrailingZeroCount((ulong)powerOf2);
        }

        /// <summary>
        /// Returns the log2 of the given int value if it is an exact power of 2 and greater than zero.
        /// </summary>
        /// <param name="powerOf2">Must be a power of 2 and greater than zero.</param>
        /// <returns>The log2 of the given int value if it is an exact power of 2 and greater than zero.</returns>
        public static int ExactLog2OfInt(int powerOf2)
        {
            if (!IsPowerOf2(powerOf2))
            {
                throw new SketchesArgumentException("Argument 'powerOf2' must be a positive power of 2.");
            }
            return BitOperations.TrailingZeroCount((uint)powerOf2);
        }

        /// <summary>
        /// Returns the log2 of the given long value if it is an exact power of 2 and greater than zero.
        /// </summary>
        /// <param name="powerOf2">Must be a power of 2 and greater than zero.</param>
        /// <returns>The log2 of the given long value if it is an exact power of 2 and greater than zero.</returns>
        public static int ExactLog2OfLong(long powerOf2)
        {
            if (!IsPowerOf2(powerOf2))
            {
                throw new SketchesArgumentException("Argument 'powerOf2' must be a positive power of 2.");
            }
            return BitOperations.TrailingZeroCount((ulong)powerOf2);
        }

        // Checks that throw

        /// <summary>
        /// Check the requested offset and length against the allocated size.
        /// The invariants equation is: 0 &lt;= reqOff &lt;= reqLen &lt;= reqOff + reqLen &lt;= allocSize.
        /// If this equation is violated an SketchesArgumentException will be thrown.
        /// </summary>
        /// <param name="reqOff">The requested offset</param>
        /// <param name="reqLen">The requested length</param>
        /// <param name="allocSize">The allocated size.</param>
        public static void CheckBounds(long reqOff, long reqLen, long allocSize)
        {
            if ((reqOff | reqLen | (reqOff + reqLen) | (allocSize - (reqOff + reqLen))) < 0)
            {
                throw new SketchesArgumentException($"Bounds Violation: " +
                    $"reqOffset: {reqOff}, reqLength: {reqLen}, " +
                    $"(reqOff + reqLen): {reqOff + reqLen}, allocSize: {allocSize}");
            }
        }

        /// <summary>
        /// Checks the given parameter to make sure it is positive and between 0.0 inclusive and 1.0 inclusive.
        /// </summary>
        /// <param name="p">The probability parameter</param>
        /// <param name="argName">Used in the thrown exception.</param>
        public static void CheckProbability(double p, string argName)
        {
            if (p >= 0.0 && p <= 1.0)
            {
                return;
            }
            throw new SketchesArgumentException($"The value of the parameter \"{argName}\" " +
                $"must be between 0.0 inclusive and 1.0 inclusive: {p}");
        }

        // Boolean Checks

        /// <summary>
        /// Unsigned compare with longs.
        /// </summary>
        /// <param name="n1">A long to be treated as if unsigned.</param>
        /// <param name="n2">A long to be treated as if unsigned.</param>
        /// <returns>True if n1 &lt; n2 (unsigned comparison).</returns>
        public static bool IsLessThanUnsigned(long n1, long n2)
        {
            return (ulong)n1 < (ulong)n2;
        }

        /// <summary>
        /// Returns true if given n is even.
        /// </summary>
        /// <param name="n">The given n</param>
        /// <returns>True if given n is even.</returns>
        public static bool IsEven(long n)
        {
            return (n & 1L) == 0;
        }

        /// <summary>
        /// Returns true if given n is odd.
        /// </summary>
        /// <param name="n">The given n</param>
        /// <returns>True if given n is odd.</returns>
        public static bool IsOdd(long n)
        {
            return (n & 1L) == 1L;
        }

        // Other

        /// <summary>
        /// Returns a one if the bit at bitPos is a one, otherwise zero.
        /// </summary>
        /// <param name="number">The number to examine</param>
        /// <param name="bitPos">The given zero-based bit position, where the least significant bit is at position zero.</param>
        /// <returns>A one if the bit at bitPos is a one, otherwise zero.</returns>
        public static int BitAt(long number, int bitPos)
        {
            return (number & (1L << bitPos)) > 0 ? 1 : 0;
        }

        /// <summary>
        /// Computes the number of decimal digits of the number n
        /// </summary>
        /// <param name="n">The given number</param>
        /// <returns>The number of decimal digits of the number n</returns>
        public static int NumDigits(long n)
        {
            if (n % 10 == 0) { n++; }
            return (int)Math.Ceiling(Math.Log(n) / Math.Log(10));
        }

        /// <summary>
        /// Converts the given number to a string prepended with spaces, if necessary, to match the given length.
        /// </summary>
        /// <param name="number">The given number</param>
        /// <param name="length">The desired string length.</param>
        /// <returns>The given number to a string prepended with spaces</returns>
        public static string LongToFixedLengthString(long number, int length)
        {
            string num = number.ToString();
            return CharacterPad(num, length, ' ', false);
        }

        // Generic comparisons

        /// <summary>
        /// Finds the minimum of two generic items
        /// </summary>
        /// <typeparam name="T">The type</typeparam>
        /// <param name="item1">Item one</param>
        /// <param name="item2">Item two</param>
        /// <param name="c">The given comparator</param>
        /// <returns>The minimum value</returns>
        public static T MinT<T>(T item1, T item2, IComparer<T> c)
        {
            return c.Compare(item1, item2) <= 0 ? item1 : item2;
        }

        /// <summary>
        /// Finds the maximum of two generic items
        /// </summary>
        /// <typeparam name="T">The type</typeparam>
        /// <param name="item1">Item one</param>
        /// <param name="item2">Item two</param>
        /// <param name="c">The given comparator</param>
        /// <returns>The maximum value</returns>
        public static T MaxT<T>(T item1, T item2, IComparer<T> c)
        {
            return c.Compare(item1, item2) >= 0 ? item1 : item2;
        }

        /// <summary>
        /// Is item1 Less-Than item2
        /// </summary>
        /// <typeparam name="T">The type</typeparam>
        /// <param name="item1">Item one</param>
        /// <param name="item2">Item two</param>
        /// <param name="c">The given comparator</param>
        /// <returns>True if item1 Less-Than item2</returns>
        public static bool Lt<T>(T item1, T item2, IComparer<T> c)
        {
            return c.Compare(item1, item2) < 0;
        }

        /// <summary>
        /// Is item1 Less-Than-Or-Equal-To item2
        /// </summary>
        /// <typeparam name="T">The type</typeparam>
        /// <param name="item1">Item one</param>
        /// <param name="item2">Item two</param>
        /// <param name="c">The given comparator</param>
        /// <returns>True if item1 Less-Than-Or-Equal-To item2</returns>
        public static bool Le<T>(T item1, T item2, IComparer<T> c)
        {
            return c.Compare(item1, item2) <= 0;
        }
    }
}
