using System.Collections.Concurrent;
using System.Threading.Channels;

namespace worker;

public static class Program
{
    public static async Task Main()
    {
        Channel<(string, int)> queue = Channel.CreateUnbounded<(string, int)>();

        var producer = Task.Run(() =>
        {
            for (int regionId = 1; regionId < 6; regionId++)
            {
                Enumerable.Range(1, 20).ToList().ForEach((val) =>
                {
                    Console.WriteLine($"Produce, Region: {regionId}, item: {val}");
                    queue.Writer.WriteAsync(($"Region {regionId}", val));
                });
            }
        });

        ConcurrentDictionary<string, Channel<int>> regions = new();

        List<Task> regionWorkers = new List<Task>();
        ConcurrentBag<Task> consumers = new();
        await foreach ((string, int) regionVpg in queue.Reader.ReadAllAsync())
        {
            var rq = regions.GetOrAdd(regionVpg.Item1, (key) =>
            {
                var rq1 = Channel.CreateUnbounded<int>();
                var w = CreateConsumer(key, rq1);
                consumers.Add(w);
                return rq1;
            });

            await rq.Writer.WriteAsync(regionVpg.Item2);
        }

        await producer;

        // See https://aka.ms/new-console-template for more information
        Console.WriteLine("Click enter to exit");
        Console.ReadLine();
    }

    private static Task CreateConsumer(string regionKey, Channel<int> rq)
    {
        return Task.Run(async () =>
        {
            List<Task> workers = new();
            while (await rq.Reader.WaitToReadAsync())
            {
                for (int i = 0; i < 5; i++)
                {
                    var workItem = await rq.Reader.ReadAsync();
                    workers.Add(Task.Run(() =>
                    {
                        Console.WriteLine($"Consume, Region: {regionKey}, worker: {workers.Count - 1}, workItem: {workItem}");
                    }));
                }
                await Task.WhenAll(workers);
                workers.Clear();
            }
        });
    }
}

