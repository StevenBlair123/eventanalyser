using System;
using System.Threading;
using Prometheus;

class Program {
    static void Main(string[] args) {
        // Start the metric server
        var server = new MetricServer(port: 1234); // Change port as needed
        server.Start();

        // Create a summary metric
        var summary = Metrics.CreateSummary("TestMetricSummary", "Will observe a value and publish it as Summary");

        // Simulate observing values
        var random = new Random();
        for (int i = 0; i < 10; i++) {
            double value = random.NextDouble(); // Generate a random value
            summary.Observe(value); // Observe the value

            // Log the value to the console
            Console.WriteLine($"Observed value: {value}");

            Thread.Sleep(1000); // Wait for a second before observing the next value
        }

        // Keep the application running
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
}