using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ProducerConsumer
{
    class Program
    {
        CountdownEvent producerWaiting;
        CountdownEvent consumerWaiting;
        static void Main(string[] args)
        {
            Program program = new Program();
            int storageSize = int.Parse(Console.ReadLine());
            int itemNumber = int.Parse(Console.ReadLine());
            int countOfProducers = int.Parse(Console.ReadLine());
            int countOfConsumers = int.Parse(Console.ReadLine());
            program.producerWaiting = new CountdownEvent(countOfProducers);
            program.consumerWaiting = new CountdownEvent(countOfConsumers);
            program.Starter(storageSize, itemNumber,countOfProducers,countOfConsumers);
            program.producerWaiting.Wait();
            program.consumerWaiting.Wait();
            Console.WriteLine("completed");
            Console.ReadKey();
        }


        private void Starter(int storageSize, int itemNumber, int countOfProducers, int countOfConsumers)
        {
            Access = new Semaphore(1, 1);
            Full = new Semaphore(storageSize, storageSize);
            Empty = new Semaphore(0, storageSize);



            for (int i = 0; i < countOfConsumers; i++)
            {
                int id = i;
                int countToTake = itemNumber / countOfConsumers;
                if (id < itemNumber % countOfConsumers) countToTake++;

                new Thread(() => Consumer(id, countToTake)).Start();
            }


            for (int i = 0; i < countOfProducers; i++)
            {
                int id = i;
                int countToProduce = itemNumber / countOfProducers;
                if (id < itemNumber % countOfProducers) countToProduce++;

                new Thread(() => Producer(id, countToProduce)).Start();
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
            producerWaiting.Signal();
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
            consumerWaiting.Signal();
        }
    }
}
