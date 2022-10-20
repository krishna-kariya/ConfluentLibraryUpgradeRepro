using System;
using System.Threading.Tasks;
using System.Threading;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace ConfluentConsumer
{
    internal class Repro
    {
        private Lazy<IConsumer<String, String>> consumer;
        private DateTime start;
        private ILogger logger;
        private CancellationTokenSource cancellationTokenSource;

        Repro()
        {
            consumer = new Lazy<IConsumer<String, String>>(() => CreateConsumer());
            logger = LoggerFactory.Create(config =>
            {
                config.AddConsole();
            }).CreateLogger("Program");
            cancellationTokenSource = new CancellationTokenSource();
        }

        static async Task Main(string[] args)
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            Repro repro = new Repro();
            await repro.StartAsync(cts.Token);
            Console.WriteLine("Hello World!");
        }

        protected virtual ConsumerBuilder<String, String> CreateConsumerBuilder(ConsumerConfig config) => new ConsumerBuilder<String, String>(config);

        private IConsumer<String, String> CreateConsumer()
        {
            var watch = System.Diagnostics.Stopwatch.StartNew();

            //AzureFunctionsFileHelper.InitializeLibrdKafka(this.logger);

            this.start = DateTime.Now;
            var builder = this.CreateConsumerBuilder(GetConsumerConfiguration());

            builder.SetErrorHandler((_, e) =>
            {
                logger.LogError(e.Reason);
            })
            .SetPartitionsAssignedHandler((_, e) =>
            {
                var end = DateTime.Now;
                var diff = end - start;
                logger.LogInformation($"Time for partition assigned: {diff.ToString()}");
                logger.LogInformation($"Assigned partitions: [{string.Join(", ", e)}]");
                start = DateTime.Now;
            })
            .SetPartitionsRevokedHandler((_, e) =>
            {
                var end = DateTime.Now;
                var diff = end - start;
                logger.LogInformation($"Time for revoke partition assigned: {diff.ToString()}");
                logger.LogInformation($"Revoked partitions: [{string.Join(", ", e)}]");
                start = DateTime.Now;
            });


            builder.SetLogHandler((_, m) =>
            {
                logger.Log((LogLevel)m.LevelAs(LogLevelType.MicrosoftExtensionsLogging), $"Libkafka: {m?.Message}");
            });

            var consumer = builder.Build();
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            this.logger.LogInformation($"Consumer Built Time taken: {elapsedMs} ms");

            return consumer;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            var localConsumer = this.consumer.Value;

            localConsumer.Subscribe("topicten");
            // Using a thread as opposed to a task since this will be long running
            var thread = new Thread(ProcessSubscription)
            {
                IsBackground = true,
            };
            thread.Start(cancellationTokenSource.Token);
            thread.Join();
            return Task.CompletedTask;
        }


        private ConsumerConfig GetConsumerConfiguration()
        {
            ConsumerConfig conf = new ConsumerConfig()
            {
                // enable auto-commit 
                EnableAutoCommit = true,

                // disable auto storing read offsets since we need to store them after calling the trigger function
                EnableAutoOffsetStore = false,

                // Interval in which commits stored in memory will be saved
                AutoCommitIntervalMs = 200,

                // Librdkafka debug options               
                Debug = null,
                // Debug = "consumer,cgrp,topic,fetch,broker,msg",

                // start from earliest if no checkpoint has been committed
                AutoOffsetReset = AutoOffsetReset.Earliest,

                // Secure communication/authentication
                //SaslMechanism = this.listenerConfiguration.SaslMechanism,
                //SaslUsername = this.listenerConfiguration.SaslUsername,
                //SaslPassword = this.listenerConfiguration.SaslPassword,
                //SecurityProtocol = this.listenerConfiguration.SecurityProtocol,
                //SslCaLocation = this.listenerConfiguration.SslCaLocation,
                //SslCertificateLocation = this.listenerConfiguration.SslCertificateLocation,
                //SslKeyLocation = this.listenerConfiguration.SslKeyLocation,
                //SslKeyPassword = this.listenerConfiguration.SslKeyPassword,

                // Values from host configuration
                StatisticsIntervalMs = null,
                ReconnectBackoffMs = null,
                ReconnectBackoffMaxMs = null,
                SessionTimeoutMs = null,
                MaxPollIntervalMs = null,
                QueuedMinMessages = null,
                QueuedMaxMessagesKbytes = null,
                MaxPartitionFetchBytes = null,
                FetchMaxBytes = null,
                MetadataMaxAgeMs = 180000,
                SocketKeepaliveEnable = true
            };

            // Setup native kafka configuration
            conf.BootstrapServers = "localhost:9092";
            conf.GroupId = "$Default";
            return conf;
        }


        private void ProcessSubscription(object parameter)
        {
            var localConsumer = this.consumer.Value;
            long timeTakenforSingleMessage = 0;
            var cancellationToken = (CancellationToken)parameter;

            while (true)
            {
                try
                {
                    var watch = System.Diagnostics.Stopwatch.StartNew();
                    var consumeResult = localConsumer.Consume(cancellationToken);
                    watch.Stop();
                    var elapsedMs = watch.ElapsedMilliseconds;
                    this.logger.LogInformation($"Time taken for this message: {timeTakenforSingleMessage} on partition: {consumeResult.Partition}");
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, "Error in Kafka subscriber");
                }
            }
        }
    }
}
