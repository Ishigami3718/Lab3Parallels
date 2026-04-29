using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ProducerConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Program program = new Program();
            int storageSize = int.Parse(Console.ReadLine());
            int itemNumber = int.Parse(Console.ReadLine());
            int countOfProducers = int.Parse(Console.ReadLine());
            int countOfConsumers = int.Parse(Console.ReadLine());
            program.Starter(storageSize, itemNumber,countOfProducers,countOfConsumers);

            Console.ReadKey();
        }


        private void Starter(int storageSize, int itemNumber, int countOfProducers, int countOfConsumers)
        {
            Access = new Semaphore(1, 1);
            Full = new Semaphore(storageSize, storageSize);
            Empty = new Semaphore(0, storageSize);


            int productCountForSingleConsumer = itemNumber / countOfConsumers;
            int remainProductsForConsumers = itemNumber % countOfConsumers;

            for (int i = 0; i < countOfConsumers; i++,remainProductsForConsumers--)
            {
                int id = i;
                new Thread(()=>Consumer(id,remainProductsForConsumers>0?
                    productCountForSingleConsumer+1:productCountForSingleConsumer)).Start();
            }

            int productCountForSingleProducer = itemNumber / countOfProducers;
            int remainProductsForProducer = itemNumber % countOfProducers;

            for (int i = 0; i < countOfProducers; i++, remainProductsForProducer--)
            {
                int id = i;
                new Thread(() => Producer(id, remainProductsForProducer > 0 ?
                    productCountForSingleProducer + 1 : productCountForSingleProducer)).Start();
            }
        }

        private Semaphore Access;
        private Semaphore Full;
        private Semaphore Empty;

        private readonly List<string> storage = new List<string>();


        private void Producer(int id,Object itemNumbers)
        {
            int maxItem = 0;
            if (itemNumbers is int)
            {
                maxItem = (int)itemNumbers;
            }
            for (int i = 0; i < maxItem; i++)
            {
                Full.WaitOne();
                Access.WaitOne();

                storage.Add("item " + i + "by producer "+id);
                Console.WriteLine("Added item " + i + "by producer " + id);
                
                Access.Release();
                Empty.Release();
                
            }
        }

        private void Consumer(int id,Object itemNumbers)
        {
            int maxItem = 0;
            if (itemNumbers is int)
            {
                maxItem = (int)itemNumbers;
            }
            for (int i = 0; i < maxItem; i++)
            {
                Empty.WaitOne();
                Access.WaitOne();
                
                string item = storage.ElementAt(0);
                storage.RemoveAt(0);
                
                Full.Release();
                Access.Release();
                
                Console.WriteLine("consumer with "+id+" Took " + item);
            }
        }
    }
}
