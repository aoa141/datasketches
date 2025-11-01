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
using System.IO;

namespace DataSketches.Common
{
    /// <summary>
    /// Common definitions and utilities for DataSketches
    /// </summary>
    public static class CommonDefs
    {
        public const ulong DEFAULT_SEED = 9001;

        public enum ResizeFactor
        {
            X1 = 0,
            X2,
            X4,
            X8
        }

        /// <summary>
        /// Compute log base 2 of a number
        /// </summary>
        public static byte Log2(uint n)
        {
            return (byte)(n > 1 ? 1 + Log2(n >> 1) : 0);
        }

        /// <summary>
        /// Compute log size from count and load factor
        /// </summary>
        public static byte LgSizeFromCount(uint n, double loadFactor)
        {
            return (byte)(Log2(n) + (n > (uint)((1 << (Log2(n) + 1)) * loadFactor) ? 2 : 1));
        }

        /// <summary>
        /// Byte swap a value for endianness conversion
        /// </summary>
        public static T ByteSwap<T>(T value) where T : struct
        {
            byte[] bytes = BitConverter.GetBytes((dynamic)value);
            Array.Reverse(bytes);
            return (T)(dynamic)BitConverter.ToInt64(bytes, 0);
        }

        /// <summary>
        /// Read a value from a stream
        /// </summary>
        public static T Read<T>(BinaryReader reader) where T : struct
        {
            int size = System.Runtime.InteropServices.Marshal.SizeOf<T>();
            byte[] bytes = reader.ReadBytes(size);

            if (typeof(T) == typeof(byte))
                return (T)(object)bytes[0];
            if (typeof(T) == typeof(sbyte))
                return (T)(object)(sbyte)bytes[0];
            if (typeof(T) == typeof(short))
                return (T)(object)BitConverter.ToInt16(bytes, 0);
            if (typeof(T) == typeof(ushort))
                return (T)(object)BitConverter.ToUInt16(bytes, 0);
            if (typeof(T) == typeof(int))
                return (T)(object)BitConverter.ToInt32(bytes, 0);
            if (typeof(T) == typeof(uint))
                return (T)(object)BitConverter.ToUInt32(bytes, 0);
            if (typeof(T) == typeof(long))
                return (T)(object)BitConverter.ToInt64(bytes, 0);
            if (typeof(T) == typeof(ulong))
                return (T)(object)BitConverter.ToUInt64(bytes, 0);
            if (typeof(T) == typeof(float))
                return (T)(object)BitConverter.ToSingle(bytes, 0);
            if (typeof(T) == typeof(double))
                return (T)(object)BitConverter.ToDouble(bytes, 0);

            throw new NotSupportedException($"Type {typeof(T)} is not supported");
        }

        /// <summary>
        /// Write a value to a stream
        /// </summary>
        public static void Write<T>(BinaryWriter writer, T value) where T : struct
        {
            if (value is byte b)
                writer.Write(b);
            else if (value is sbyte sb)
                writer.Write(sb);
            else if (value is short s)
                writer.Write(s);
            else if (value is ushort us)
                writer.Write(us);
            else if (value is int i)
                writer.Write(i);
            else if (value is uint ui)
                writer.Write(ui);
            else if (value is long l)
                writer.Write(l);
            else if (value is ulong ul)
                writer.Write(ul);
            else if (value is float f)
                writer.Write(f);
            else if (value is double d)
                writer.Write(d);
            else
                throw new NotSupportedException($"Type {typeof(T)} is not supported");
        }

        /// <summary>
        /// Read a big-endian value from a stream
        /// </summary>
        public static T ReadBigEndian<T>(BinaryReader reader) where T : struct
        {
            T value = Read<T>(reader);
            if (!BitConverter.IsLittleEndian)
                return value;
            return ByteSwap(value);
        }
    }

    /// <summary>
    /// Random number utilities
    /// </summary>
    public static class RandomUtils
    {
        [ThreadStatic]
        private static Random? _random;

        private static Random GetRandom()
        {
            if (_random == null)
            {
                _random = new Random();
            }
            return _random;
        }

        public static double NextDouble()
        {
            return GetRandom().NextDouble();
        }

        public static ulong NextUInt64()
        {
            var buffer = new byte[8];
            GetRandom().NextBytes(buffer);
            return BitConverter.ToUInt64(buffer, 0);
        }

        public static bool NextBit()
        {
            return GetRandom().Next(2) == 1;
        }

        public static void OverrideSeed(int seed)
        {
            _random = new Random(seed);
        }
    }
}
