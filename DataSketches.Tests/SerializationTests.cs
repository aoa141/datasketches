using Apache.DataSketches.Theta;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace Apache.DataSketches.Tests
{
    [TestClass]
    public class SerializationTests
    {
        #region UpdateSketch Serialization Tests

        [TestMethod]
        public void TestUpdateSketch_EmptySketch_Serialization()
        {
            // Create empty sketch
            var builder = new UpdateSketchBuilder();
            var originalSketch = builder.Build();

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);
            Assert.IsTrue(serialized.Length > 0);

            // Deserialize
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify
            Assert.IsTrue(deserializedSketch.IsEmpty());
            Assert.AreEqual(originalSketch.GetEstimate(), deserializedSketch.GetEstimate(), 0.0);
            Assert.AreEqual(originalSketch.GetRetainedEntries(), deserializedSketch.GetRetainedEntries());
            Assert.AreEqual(originalSketch.GetThetaLong(), deserializedSketch.GetThetaLong());
        }

        [TestMethod]
        public void TestUpdateSketch_SingleItem_Serialization()
        {
            // Create sketch with single item
            var builder = new UpdateSketchBuilder();
            var originalSketch = builder.Build();
            originalSketch.Update(42L);

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify
            Assert.IsFalse(deserializedSketch.IsEmpty());
            Assert.AreEqual(1.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.AreEqual(1, deserializedSketch.GetRetainedEntries());
            Assert.AreEqual(originalSketch.GetThetaLong(), deserializedSketch.GetThetaLong());
        }

        [TestMethod]
        public void TestUpdateSketch_MultipleItems_Serialization()
        {
            // Create sketch with multiple items
            var builder = new UpdateSketchBuilder();
            var originalSketch = builder.Build();
            for (int i = 0; i < 100; i++)
            {
                originalSketch.Update(i);
            }

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify
            Assert.IsFalse(deserializedSketch.IsEmpty());
            Assert.AreEqual(100.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.AreEqual(originalSketch.GetRetainedEntries(), deserializedSketch.GetRetainedEntries());
            Assert.AreEqual(originalSketch.GetThetaLong(), deserializedSketch.GetThetaLong());
        }

        [TestMethod]
        public void TestUpdateSketch_LargeDataSet_Serialization()
        {
            // Create sketch with large dataset (forces estimation mode)
            var builder = new UpdateSketchBuilder().SetNominalEntries(4096);
            var originalSketch = builder.Build();
            for (int i = 0; i < 10000; i++)
            {
                originalSketch.Update(i);
            }

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify
            Assert.IsFalse(deserializedSketch.IsEmpty());
            double originalEstimate = originalSketch.GetEstimate();
            double deserializedEstimate = deserializedSketch.GetEstimate();
            Assert.AreEqual(originalEstimate, deserializedEstimate, 0.01);
            Assert.AreEqual(originalSketch.GetRetainedEntries(), deserializedSketch.GetRetainedEntries());
            Assert.AreEqual(originalSketch.GetThetaLong(), deserializedSketch.GetThetaLong());
        }

        [TestMethod]
        public void TestUpdateSketch_WithStrings_Serialization()
        {
            // Create sketch with string data
            var builder = new UpdateSketchBuilder();
            var originalSketch = builder.Build();
            originalSketch.Update("apple");
            originalSketch.Update("banana");
            originalSketch.Update("cherry");
            originalSketch.Update("date");

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify
            Assert.AreEqual(4.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.AreEqual(originalSketch.GetRetainedEntries(), deserializedSketch.GetRetainedEntries());
            Assert.AreEqual(originalSketch.GetThetaLong(), deserializedSketch.GetThetaLong());
        }

        [TestMethod]
        public void TestUpdateSketch_WithDoubles_Serialization()
        {
            // Create sketch with double data
            var builder = new UpdateSketchBuilder();
            var originalSketch = builder.Build();
            originalSketch.Update(1.5);
            originalSketch.Update(2.5);
            originalSketch.Update(3.5);

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();

            // Deserialize
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify
            Assert.AreEqual(3.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.AreEqual(originalSketch.GetRetainedEntries(), deserializedSketch.GetRetainedEntries());
        }

        [TestMethod]
        public void TestUpdateSketch_CustomNominalEntries_Serialization()
        {
            // Create sketch with custom nominal entries
            var builder = new UpdateSketchBuilder().SetNominalEntries(512);
            var originalSketch = builder.Build();
            for (int i = 0; i < 50; i++)
            {
                originalSketch.Update(i);
            }

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();

            // Deserialize
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify
            Assert.AreEqual(50.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.AreEqual(originalSketch.GetLgNomLongs(), deserializedSketch.GetLgNomLongs());
        }

        #endregion

        #region CompactSketch Serialization Tests

        [TestMethod]
        public void TestCompactSketch_Empty_Serialization()
        {
            // Create empty compact sketch
            var builder = new UpdateSketchBuilder();
            var updateSketch = builder.Build();
            var originalSketch = updateSketch.Compact();

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedSketch = CompactSketch.Heapify(serialized);

            // Verify
            Assert.IsTrue(deserializedSketch.IsEmpty());
            Assert.AreEqual(0.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.IsTrue(deserializedSketch.IsCompact());
        }

        [TestMethod]
        public void TestCompactSketch_WithData_Serialization()
        {
            // Create compact sketch with data
            var builder = new UpdateSketchBuilder();
            var updateSketch = builder.Build();
            for (int i = 0; i < 50; i++)
            {
                updateSketch.Update(i);
            }
            var originalSketch = updateSketch.Compact();

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedSketch = CompactSketch.Heapify(serialized);

            // Verify
            Assert.IsFalse(deserializedSketch.IsEmpty());
            Assert.AreEqual(50.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.IsTrue(deserializedSketch.IsCompact());
            Assert.AreEqual(originalSketch.GetRetainedEntries(), deserializedSketch.GetRetainedEntries());
        }

        [TestMethod]
        public void TestCompactSketch_Ordered_Serialization()
        {
            // Create ordered compact sketch
            var builder = new UpdateSketchBuilder();
            var updateSketch = builder.Build();
            for (int i = 0; i < 25; i++)
            {
                updateSketch.Update(i);
            }
            var originalSketch = updateSketch.Compact(true, Span<byte>.Empty);

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedSketch = CompactSketch.Heapify(serialized);

            // Verify
            Assert.AreEqual(25.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.IsTrue(deserializedSketch.IsOrdered());
            Assert.IsTrue(deserializedSketch.IsCompact());
        }

        [TestMethod]
        public void TestCompactSketch_LargeDataSet_Serialization()
        {
            // Create compact sketch with large dataset
            var builder = new UpdateSketchBuilder().SetNominalEntries(2048);
            var updateSketch = builder.Build();
            for (int i = 0; i < 5000; i++)
            {
                updateSketch.Update(i);
            }
            var originalSketch = updateSketch.Compact();

            // Serialize
            byte[] serialized = originalSketch.ToByteArray();

            // Deserialize
            var deserializedSketch = CompactSketch.Heapify(serialized);

            // Verify
            double originalEstimate = originalSketch.GetEstimate();
            double deserializedEstimate = deserializedSketch.GetEstimate();
            Assert.AreEqual(originalEstimate, deserializedEstimate, 1.0);
            Assert.IsTrue(deserializedSketch.IsCompact());
        }

        [TestMethod]
        public void TestCompactSketch_Iterator_AfterDeserialization()
        {
            // Create compact sketch
            var builder = new UpdateSketchBuilder();
            var updateSketch = builder.Build();
            updateSketch.Update(1L);
            updateSketch.Update(2L);
            updateSketch.Update(3L);
            var originalSketch = updateSketch.Compact();

            // Serialize and deserialize
            byte[] serialized = originalSketch.ToByteArray();
            var deserializedSketch = CompactSketch.Heapify(serialized);

            // Verify iterator works
            var iterator = deserializedSketch.Iterator();
            int count = 0;
            while (iterator.Next())
            {
                count++;
                Assert.IsTrue(iterator.Get() > 0);
            }
            Assert.AreEqual(3, count);
        }

        #endregion

        #region Union Serialization Tests

        [TestMethod]
        public void TestUnion_Empty_Serialization()
        {
            // Create empty union
            var union = new SetOperationBuilder().BuildUnion();

            // Serialize
            byte[] serialized = union.ToByteArray();
            Assert.IsNotNull(serialized);
            Assert.IsTrue(serialized.Length > 0);

            // Deserialize
            var deserializedUnion = (Union)SetOperation.Heapify(new Memory<byte>(serialized));

            // Verify
            var result = deserializedUnion.GetResult();
            Assert.IsTrue(result.IsEmpty());
            Assert.AreEqual(0.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_WithData_Serialization()
        {
            // Create union with data
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);

            // Serialize
            byte[] serialized = union.ToByteArray();
            Assert.IsNotNull(serialized);

            // Deserialize
            var deserializedUnion = (Union)SetOperation.Heapify(new Memory<byte>(serialized));

            // Verify
            var result = deserializedUnion.GetResult();
            Assert.AreEqual(3.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_MultipleSketches_Serialization()
        {
            // Create union with multiple sketches
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            // Serialize
            byte[] serialized = union.ToByteArray();

            // Deserialize
            var deserializedUnion = (Union)SetOperation.Heapify(new Memory<byte>(serialized));

            // Verify
            var result = deserializedUnion.GetResult();
            Assert.AreEqual(4.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_LargeDataSet_Serialization()
        {
            // Create union with large dataset
            var builder = new UpdateSketchBuilder().SetNominalEntries(4096);
            var sketch1 = builder.Build();
            for (int i = 0; i < 5000; i++)
            {
                sketch1.Update(i);
            }

            var sketch2 = builder.Build();
            for (int i = 2500; i < 7500; i++)
            {
                sketch2.Update(i);
            }

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            // Serialize
            byte[] serialized = union.ToByteArray();

            // Deserialize
            var deserializedUnion = (Union)SetOperation.Heapify(new Memory<byte>(serialized));

            // Verify
            var originalResult = union.GetResult();
            var deserializedResult = deserializedUnion.GetResult();
            Assert.AreEqual(originalResult.GetEstimate(), deserializedResult.GetEstimate(), 10.0);
        }

        [TestMethod]
        public void TestUnion_ContinueAfterDeserialization()
        {
            // Create union with data
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);

            // Serialize and deserialize
            byte[] serialized = union.ToByteArray();
            var deserializedUnion = (Union)SetOperation.Heapify(new Memory<byte>(serialized));

            // Add more data to deserialized union
            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);
            deserializedUnion.Update(sketch2);

            // Verify
            var result = deserializedUnion.GetResult();
            Assert.AreEqual(4.0, result.GetEstimate(), 0.0);
        }

        #endregion

        #region Intersection Serialization Tests

        [TestMethod]
        public void TestIntersection_Empty_Serialization()
        {
            // Create empty intersection
            var intersection = new SetOperationBuilder().BuildIntersection();

            // Serialize
            byte[] serialized = intersection.ToByteArray();
            Assert.IsNotNull(serialized);
            Assert.IsTrue(serialized.Length > 0);

            // Note: Intersection doesn't have a Heapify method in the current implementation
            // We'll test serialization for now
        }

        [TestMethod]
        public void TestIntersection_WithData_Serialization()
        {
            // Create intersection with data
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var sketch2 = builder.Build();
            sketch2.Update(2L);
            sketch2.Update(3L);
            sketch2.Update(4L);

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            // Serialize
            byte[] serialized = intersection.ToByteArray();
            Assert.IsNotNull(serialized);
            Assert.IsTrue(serialized.Length > 0);

            // Verify we can get result before serialization
            var result = intersection.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_MultipleIntersections_Serialization()
        {
            // Create intersection with multiple sketches
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            for (int i = 1; i <= 10; i++)
            {
                sketch1.Update(i);
            }

            var sketch2 = builder.Build();
            for (int i = 5; i <= 15; i++)
            {
                sketch2.Update(i);
            }

            var sketch3 = builder.Build();
            for (int i = 7; i <= 20; i++)
            {
                sketch3.Update(i);
            }

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);
            intersection.Intersect(sketch3);

            // Serialize
            byte[] serialized = intersection.ToByteArray();
            Assert.IsNotNull(serialized);

            // Verify result
            var result = intersection.GetResult();
            Assert.AreEqual(4.0, result.GetEstimate(), 0.0); // 7, 8, 9, 10
        }

        #endregion

        #region Edge Cases and Error Handling

        [TestMethod]
        public void TestUpdateSketch_RoundTrip_PreservesProperties()
        {
            // Create sketch with specific properties
            var builder = new UpdateSketchBuilder().SetNominalEntries(1024);
            var originalSketch = builder.Build();
            for (int i = 0; i < 500; i++)
            {
                originalSketch.Update(i);
            }

            // Round trip
            byte[] serialized = originalSketch.ToByteArray();
            var deserializedSketch = UpdateSketch.Heapify(serialized);

            // Verify all properties
            Assert.AreEqual(originalSketch.IsEmpty(), deserializedSketch.IsEmpty());
            Assert.AreEqual(originalSketch.GetEstimate(), deserializedSketch.GetEstimate(), 0.01);
            Assert.AreEqual(originalSketch.GetRetainedEntries(), deserializedSketch.GetRetainedEntries());
            Assert.AreEqual(originalSketch.GetThetaLong(), deserializedSketch.GetThetaLong());
            Assert.AreEqual(originalSketch.GetLgNomLongs(), deserializedSketch.GetLgNomLongs());
            Assert.AreEqual(originalSketch.IsEstimationMode(), deserializedSketch.IsEstimationMode());
        }

        [TestMethod]
        public void TestCompactSketch_RoundTrip_PreservesOrder()
        {
            // Create ordered compact sketch
            var builder = new UpdateSketchBuilder();
            var updateSketch = builder.Build();
            for (int i = 0; i < 10; i++)
            {
                updateSketch.Update(i);
            }
            var originalSketch = updateSketch.Compact(true, Span<byte>.Empty);

            // Round trip
            byte[] serialized = originalSketch.ToByteArray();
            var deserializedSketch = CompactSketch.Heapify(serialized);

            // Verify order is preserved
            Assert.IsTrue(deserializedSketch.IsOrdered());
            Assert.AreEqual(originalSketch.GetEstimate(), deserializedSketch.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUpdateSketch_SerializedSize_Reasonable()
        {
            // Test that serialized size is reasonable
            // Note: UpdateSketch serializes the entire hash table, not just valid entries
            var builder = new UpdateSketchBuilder();
            var sketch = builder.Build();
            for (int i = 0; i < 100; i++)
            {
                sketch.Update(i);
            }

            byte[] serialized = sketch.ToByteArray();

            // UpdateSketch includes metadata and the full hash table
            // Size should be greater than 0 and reasonable
            Assert.IsTrue(serialized.Length > 0,
                $"Serialized size must be greater than 0");

            // Verify the size is consistent with the sketch's GetCurrentBytes()
            int expectedSize = sketch.GetCurrentBytes();
            Assert.AreEqual(expectedSize, serialized.Length,
                $"Serialized size {serialized.Length} should match GetCurrentBytes() {expectedSize}");
        }

        [TestMethod]
        public void TestSketch_GenericHeapify_UpdateSketch()
        {
            // Test using the generic Sketch.Heapify method
            var builder = new UpdateSketchBuilder();
            var originalSketch = builder.Build();
            originalSketch.Update(1L);
            originalSketch.Update(2L);
            originalSketch.Update(3L);

            byte[] serialized = originalSketch.ToByteArray();
            var deserializedSketch = Sketch.Heapify(serialized);

            Assert.IsNotNull(deserializedSketch);
            Assert.AreEqual(3.0, deserializedSketch.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestSketch_GenericHeapify_CompactSketch()
        {
            // Test using the generic Sketch.Heapify method with CompactSketch
            var builder = new UpdateSketchBuilder();
            var updateSketch = builder.Build();
            updateSketch.Update(5L);
            updateSketch.Update(10L);
            var compactSketch = updateSketch.Compact();

            byte[] serialized = compactSketch.ToByteArray();
            var deserializedSketch = Sketch.Heapify(serialized);

            Assert.IsNotNull(deserializedSketch);
            Assert.AreEqual(2.0, deserializedSketch.GetEstimate(), 0.0);
            Assert.IsTrue(deserializedSketch.IsCompact());
        }

        [TestMethod]
        public void TestUpdateSketch_MultipleSerializations_Consistent()
        {
            // Test that multiple serializations produce consistent results
            var builder = new UpdateSketchBuilder();
            var sketch = builder.Build();
            sketch.Update(1L);
            sketch.Update(2L);
            sketch.Update(3L);

            byte[] serialized1 = sketch.ToByteArray();
            byte[] serialized2 = sketch.ToByteArray();

            // The byte arrays should be identical
            Assert.AreEqual(serialized1.Length, serialized2.Length);
            for (int i = 0; i < serialized1.Length; i++)
            {
                Assert.AreEqual(serialized1[i], serialized2[i],
                    $"Byte mismatch at index {i}");
            }
        }

        #endregion
    }
}
