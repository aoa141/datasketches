using Apache.DataSketches.Theta;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.DataSketches.Tests
{
    [TestClass]
    public class UnionTests
    {
        [TestMethod]
        public void TestUnion_EmptySketches()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            var sketch2 = builder.Build();

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            var result = union.GetResult();
            Assert.IsTrue(result.IsEmpty());
            Assert.AreEqual(0.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_OneEmptySketch()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var sketch2 = builder.Build();

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            var result = union.GetResult();
            Assert.AreEqual(3.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_NoOverlap()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var sketch2 = builder.Build();
            sketch2.Update(4L);
            sketch2.Update(5L);
            sketch2.Update(6L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            var result = union.GetResult();
            Assert.AreEqual(6.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_PartialOverlap()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);
            sketch1.Update(4L);

            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);
            sketch2.Update(5L);
            sketch2.Update(6L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            var result = union.GetResult();
            Assert.AreEqual(6.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_CompleteOverlap()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var sketch2 = builder.Build();
            sketch2.Update(1L);
            sketch2.Update(2L);
            sketch2.Update(3L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            var result = union.GetResult();
            Assert.AreEqual(3.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_MultipleUnions()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);

            var sketch3 = builder.Build();
            sketch3.Update(5L);
            sketch3.Update(6L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);
            union.Update(sketch3);

            var result = union.GetResult();
            Assert.AreEqual(6.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_WithStrings()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update("apple");
            sketch1.Update("banana");

            var sketch2 = builder.Build();
            sketch2.Update("cherry");
            sketch2.Update("date");

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            var result = union.GetResult();
            Assert.AreEqual(4.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_UpdateWithLong()
        {
            var union = new SetOperationBuilder().BuildUnion();
            union.Update(1L);
            union.Update(2L);
            union.Update(3L);
            union.Update(2L);

            var result = union.GetResult();
            Assert.AreEqual(3.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_UpdateWithDouble()
        {
            var union = new SetOperationBuilder().BuildUnion();
            union.Update(1.5);
            union.Update(2.5);
            union.Update(3.5);
            union.Update(2.5);

            var result = union.GetResult();
            Assert.AreEqual(3.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_UpdateWithString()
        {
            var union = new SetOperationBuilder().BuildUnion();
            union.Update("hello");
            union.Update("world");
            union.Update("test");
            union.Update("hello");

            var result = union.GetResult();
            Assert.AreEqual(3.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_UpdateWithByteArray()
        {
            var union = new SetOperationBuilder().BuildUnion();
            union.Update(new byte[] { 1, 2, 3 });
            union.Update(new byte[] { 4, 5, 6 });
            union.Update(new byte[] { 1, 2, 3 });

            var result = union.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_UpdateWithIntArray()
        {
            var union = new SetOperationBuilder().BuildUnion();
            union.Update(new int[] { 1, 2, 3 });
            union.Update(new int[] { 4, 5, 6 });
            union.Update(new int[] { 1, 2, 3 });

            var result = union.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_UpdateWithLongArray()
        {
            var union = new SetOperationBuilder().BuildUnion();
            union.Update(new long[] { 1L, 2L, 3L });
            union.Update(new long[] { 4L, 5L, 6L });
            union.Update(new long[] { 1L, 2L, 3L });

            var result = union.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_UpdateWithCharArray()
        {
            var union = new SetOperationBuilder().BuildUnion();
            union.Update(new char[] { 'a', 'b', 'c' });
            union.Update(new char[] { 'd', 'e', 'f' });
            union.Update(new char[] { 'a', 'b', 'c' });

            var result = union.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_Reset()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);

            var result1 = union.GetResult();
            Assert.AreEqual(2.0, result1.GetEstimate(), 0.0);

            union.Reset();

            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);
            sketch2.Update(5L);

            union.Update(sketch2);

            var result2 = union.GetResult();
            Assert.AreEqual(3.0, result2.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_StatelessCombine()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);
            sketch2.Update(5L);

            var union = new SetOperationBuilder().BuildUnion();
            var result = union.Combine(sketch1, sketch2);

            Assert.AreEqual(5.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_LargeDataSet()
        {
            var builder = new UpdateSketchBuilder().SetNominalEntries(4096);
            var sketch1 = builder.Build();
            var sketch2 = builder.Build();

            for (int i = 0; i < 10000; i++)
            {
                sketch1.Update(i);
            }

            for (int i = 5000; i < 15000; i++)
            {
                sketch2.Update(i);
            }

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(sketch2);

            var result = union.GetResult();
            double estimate = result.GetEstimate();

            Assert.IsTrue(estimate >= 14000 && estimate <= 16000, $"Expected ~15000, got {estimate}");
        }

        [TestMethod]
        public void TestUnion_ToByteArray()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);

            byte[] bytes = union.ToByteArray();
            Assert.IsNotNull(bytes);
            Assert.IsTrue(bytes.Length > 0);
        }

        [TestMethod]
        public void TestUnion_GetCurrentBytes()
        {
            var union = new SetOperationBuilder().BuildUnion();
            int currentBytes = union.GetCurrentBytes();
            Assert.IsTrue(currentBytes > 0);
        }

        [TestMethod]
        public void TestUnion_GetMaxUnionBytes()
        {
            var union = new SetOperationBuilder().BuildUnion();
            int maxBytes = union.GetMaxUnionBytes();
            Assert.IsTrue(maxBytes > 0);
        }

        [TestMethod]
        public void TestUnion_ContinueUpdatingAfterGetResult()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);

            var result1 = union.GetResult();
            Assert.AreEqual(2.0, result1.GetEstimate(), 0.0);

            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);

            union.Update(sketch2);

            var result2 = union.GetResult();
            Assert.AreEqual(4.0, result2.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_OrderedResult()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var sketch2 = builder.Build();
            sketch2.Update(3L);
            sketch2.Update(4L);

            var union = new SetOperationBuilder().BuildUnion();
            var result = union.Combine(sketch1, sketch2, true, null);

            Assert.IsTrue(result.IsOrdered());
            Assert.AreEqual(4.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestUnion_MixedUpdateTypes()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var union = new SetOperationBuilder().BuildUnion();
            union.Update(sketch1);
            union.Update(3L);
            union.Update(4.5);
            union.Update("test");

            var result = union.GetResult();
            Assert.AreEqual(5.0, result.GetEstimate(), 0.0);
        }
    }
}
