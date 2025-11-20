using Apache.DataSketches.Theta;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.DataSketches.Tests
{
    [TestClass]
    public class IntersectionTests
    {
        [TestMethod]
        public void TestIntersection_EmptySketches()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            var sketch2 = builder.Build();

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            Assert.IsTrue(result.IsEmpty());
            Assert.AreEqual(0.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_OneEmptySketch()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var sketch2 = builder.Build();

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            Assert.IsTrue(result.IsEmpty());
            Assert.AreEqual(0.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_NoOverlap()
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

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            Assert.AreEqual(0.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_PartialOverlap()
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

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_CompleteOverlap()
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

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            Assert.AreEqual(3.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_MultipleIntersections()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);
            sketch1.Update(4L);

            var sketch2 = builder.Build();
            sketch2.Update(2L);
            sketch2.Update(3L);
            sketch2.Update(4L);
            sketch2.Update(5L);

            var sketch3 = builder.Build();
            sketch3.Update(3L);
            sketch3.Update(4L);
            sketch3.Update(5L);
            sketch3.Update(6L);

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);
            intersection.Intersect(sketch3);

            var result = intersection.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_WithStrings()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update("apple");
            sketch1.Update("banana");
            sketch1.Update("cherry");

            var sketch2 = builder.Build();
            sketch2.Update("banana");
            sketch2.Update("cherry");
            sketch2.Update("date");

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_StatelessPairwise()
        {
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
            var result = intersection.Intersect(sketch1, sketch2);

            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_Reset()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);

            var sketch2 = builder.Build();
            sketch2.Update(2L);
            sketch2.Update(3L);

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result1 = intersection.GetResult();
            Assert.AreEqual(1.0, result1.GetEstimate(), 0.0);

            intersection.Reset();

            var sketch3 = builder.Build();
            sketch3.Update(5L);
            sketch3.Update(6L);

            var sketch4 = builder.Build();
            sketch4.Update(5L);
            sketch4.Update(7L);

            intersection.Intersect(sketch3);
            intersection.Intersect(sketch4);

            var result2 = intersection.GetResult();
            Assert.AreEqual(1.0, result2.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_HasResult()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);

            var intersection = new SetOperationBuilder().BuildIntersection();
            Assert.IsFalse(intersection.HasResult());

            intersection.Intersect(sketch1);
            Assert.IsTrue(intersection.HasResult());
        }

        [TestMethod]
        public void TestIntersection_LargeDataSet()
        {
            var builder = new UpdateSketchBuilder().SetNominalEntries(4096);
            var sketch1 = builder.Build();
            var sketch2 = builder.Build();

            for (int i = 0; i < 10000; i++)
            {
                sketch1.Update(i);
                if (i >= 5000)
                {
                    sketch2.Update(i);
                }
            }

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            double estimate = result.GetEstimate();

            Assert.IsTrue(estimate >= 4500 && estimate <= 5500, $"Expected ~5000, got {estimate}");
        }

        [TestMethod]
        public void TestIntersection_ToByteArray()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1L);
            sketch1.Update(2L);
            sketch1.Update(3L);

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);

            byte[] bytes = intersection.ToByteArray();
            Assert.IsNotNull(bytes);
            Assert.IsTrue(bytes.Length > 0);
        }

        [TestMethod]
        public void TestIntersection_WithDoubles()
        {
            var builder = new UpdateSketchBuilder();
            var sketch1 = builder.Build();
            sketch1.Update(1.5);
            sketch1.Update(2.5);
            sketch1.Update(3.5);

            var sketch2 = builder.Build();
            sketch2.Update(2.5);
            sketch2.Update(3.5);
            sketch2.Update(4.5);

            var intersection = new SetOperationBuilder().BuildIntersection();
            intersection.Intersect(sketch1);
            intersection.Intersect(sketch2);

            var result = intersection.GetResult();
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }

        [TestMethod]
        public void TestIntersection_OrderedResult()
        {
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
            var result = intersection.Intersect(sketch1, sketch2, true, null);

            Assert.IsTrue(result.IsOrdered());
            Assert.AreEqual(2.0, result.GetEstimate(), 0.0);
        }
    }
}
