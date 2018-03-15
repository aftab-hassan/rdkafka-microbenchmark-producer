using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RdKafka;

namespace RdKafkaDotNetProducerMicroBenchmark
{
    class Program
    {
        public static void Main(string[] args)
        {
            //string brokerList = "138.91.140.244:9092";//d4
            string brokerList = "40.118.249.252:9092";//b2
            //string brokerList = "localhost";
            string topicName = "testBench24";
            int deviceCount = 0;

            Console.ReadLine();
            using (Producer producer = new Producer(brokerList))
            using (Topic topic = producer.Topic(topicName))
            {
                //Console.WriteLine($"{producer.Name} producing on {topic.Name}. q to exit.");

                string text;
                DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

                while ((text = Guid.NewGuid() + "_" + Math.Floor((DateTime.Now.ToUniversalTime() - origin).TotalMilliseconds)) != "q")
                {
                    //Console.WriteLine(text);
                    byte[] data = Encoding.UTF8.GetBytes(text);
                    Task<DeliveryReport> deliveryReport = topic.Produce(data);
                    var unused = deliveryReport.ContinueWith(task =>
                    {
                        //Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                    });

                    Console.WriteLine("deviceCount == " + deviceCount);
                    if (deviceCount == 1000000)
                        break;
                    deviceCount++;
                }
            }

            Console.ReadLine();
        }
    }
}
