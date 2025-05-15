using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Queues;
using Azure.Storage.Queues.Models;
using Azure.Storage.Sas;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text.Json;

namespace AzTinyCopier
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IHostApplicationLifetime _hostApplicationLifetime;
        private TelemetryClient _telemetryClient;
        private Config _config;

        public Worker(ILogger<Worker> logger,
            IHostApplicationLifetime hostApplicationLifetime,
            TelemetryClient telemetryClient,
            Config config)
        {
            _logger = logger;
            _hostApplicationLifetime = hostApplicationLifetime;
            _telemetryClient = telemetryClient;
            _config = config;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            stoppingToken.Register(() =>
            {
                _logger.LogInformation("Worker Cancelling");
            });

            try
            {
                while (true)
                {
                    if (!await ProcessQueueMessage(stoppingToken))
                    {
                        _logger.LogInformation("Sleeping");
                        await Task.Delay(TimeSpan.FromMinutes(_config.SleepWait));
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Operation Canceled");
            }
            catch (Exception ex)
            {
                _telemetryClient.TrackException(ex);
                _logger.LogError(ex, "Unhandled Exception");
            }
            finally
            {
                _logger.LogInformation("Flushing App Insights");
                _telemetryClient.Flush();
                Task.Delay(5000).Wait();

                _hostApplicationLifetime.StopApplication();
            }

        }

        protected async Task<bool> ProcessQueueMessage(CancellationToken cancellationToken)
        {
            var queueClient = new QueueClient(_config.OperationConnection, _config.QueueName);
            QueueMessage queueMessage = null;
            Message msg = null;

            using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("GetMessage"))
            {

                op.Telemetry.Properties.Add("Run", _config.Run);
                op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());
                op.Telemetry.Properties.Add("OperationConnection", queueClient.AccountName);
                op.Telemetry.Properties.Add("QueueName", _config.QueueName);
                op.Telemetry.Properties.Add("VisibilityTimeout", _config.VisibilityTimeout.ToString());

                await queueClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

                if (queueClient.Exists())
                {
                    queueMessage = await queueClient.ReceiveMessageAsync(TimeSpan.FromMinutes(_config.VisibilityTimeout), cancellationToken);

                    if (queueMessage != null)
                    {
                        msg = Message.FromString(queueMessage.MessageText);
                    }
                }

                op.Telemetry.Properties.Add("QueueEmpty", (queueMessage == null).ToString());
            }

            if (msg == null)
            {
                return false;
            }


            if (msg.Action.Equals("ProcessAccount", StringComparison.InvariantCultureIgnoreCase))
            {
                _logger.LogInformation("ProcessAccount");
                //Create one queue message for each container in the source account
                using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessAccount"))
                {
                    var sourceBlobServiceClient = new BlobServiceClient(_config.SourceConnection);
                    int containerCount = 0;

                    foreach (var container in sourceBlobServiceClient.GetBlobContainers())
                    {
                        await queueClient.SendMessageAsync((new Message()
                        {
                            Action = "ProcessPath",
                            Container = container.Name,
                            Path = string.Empty
                        }).ToString());
                        containerCount++;
                    }

                    op.Telemetry.Properties.Add("Run", _config.Run);
                    op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());
                    op.Telemetry.Properties.Add("SourceConnection", sourceBlobServiceClient.AccountName);
                    op.Telemetry.Properties.Add("containerCount", containerCount.ToString());
                }
            }
            else if (msg.Action.Equals("ProcessPath", StringComparison.InvariantCultureIgnoreCase))
            {
                _logger.LogInformation($"ProcessPath: {msg.Container} {msg.Path}");
                using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessPath"))
                {
                    var sourceBlobServiceClient = new BlobServiceClient(_config.SourceConnection);
                    var sourceBlobContainerClient = sourceBlobServiceClient.GetBlobContainerClient(msg.Container);
                    var sasBuilder = new BlobSasBuilder()
                    {
                        BlobContainerName = msg.Container,
                        Resource = "c",
                        ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(_config.VisibilityTimeout)
                    };
                    sasBuilder.SetPermissions(BlobAccountSasPermissions.Read);
                    Uri sasUri = sourceBlobContainerClient.GenerateSasUri(sasBuilder);
                    
                    var destinationBlobServiceClient = new BlobServiceClient(_config.DestinationConnection);
                    var destinationBlobContainerClient = destinationBlobServiceClient.GetBlobContainerClient(msg.Container);
                    await destinationBlobContainerClient.CreateIfNotExistsAsync();

                    var operationBlobServiceClient = new BlobServiceClient(_config.OperationConnection);
                    var operationBlobContainerClient = operationBlobServiceClient.GetBlobContainerClient(msg.Container);
                    await operationBlobContainerClient.CreateIfNotExistsAsync();


                    long blobCount = 0;
                    long blobBytes = 0;
                    long blobCountMoved = 0;
                    long blobBytesMoved = 0;
                    long subPrefixes = 0;
                    string fileName = "status.csv";

                    if (string.IsNullOrEmpty(_config.Delimiter))
                    {
                        _config.Delimiter = "/";
                    }

                    if (_config.ThreadCount < 1)
                    {
                        _config.ThreadCount = Environment.ProcessorCount * 8;
                    }
                var slim = new SemaphoreSlim(_config.ThreadCount);
                var blobSet = new ConcurrentBag<Task>();
                 var getSourceTask = Task.Run(async () =>
                    {
                        var batch = new List<string>();
                        const int maxLogicalBatchSize = 100000;

                        await foreach (var item in sourceBlobContainerClient.GetBlobsByHierarchyAsync(prefix: msg.Path, delimiter: _config.Delimiter, cancellationToken: cancellationToken))
                        {
                            if (item.IsPrefix)
                            {
                                var messageText = new Message()
                                {
                                    Action = "ProcessPath",
                                    Container = msg.Container,
                                    Path = item.Prefix
                                }.ToString();

                                batch.Add(messageText);
                                subPrefixes++;

                                if (batch.Count >= maxLogicalBatchSize)
                                {
                                    await SendMessagesInChunksAsync(queueClient, batch, cancellationToken);
                                    batch.Clear();
                                }
                            }
                           else if (item.IsBlob)
                            {
                                var blobName = item.Blob.Name;
                                var blobClient = sourceBlobContainerClient.GetBlobClient(blobName);

                                blobSet.Add(Task.Run(async () =>
                                {
                                    await slim.WaitAsync(cancellationToken);
                                    try
                                    {
                                        var downloadResponse = await blobClient.DownloadAsync(cancellationToken: cancellationToken);
                                        using var reader = new StreamReader(downloadResponse.Value.Content);
                                        var content = await reader.ReadToEndAsync();

                                        try
                                        {
                                            var json = JsonDocument.Parse(content).RootElement;

                                            var startDate = DateTime.UtcNow.Date.AddDays(-30);
                                            var endDate = DateTime.UtcNow.Date.AddDays(1).AddTicks(-1);

                                            if (json.TryGetProperty("consentCreationDate", out var dateElement) &&
                                                json.TryGetProperty("isAnonymous", out var isAnonElement) &&
                                                isAnonElement.ValueKind == JsonValueKind.False &&
                                                dateElement.TryGetDateTime(out var documentDate) &&
                                                documentDate >= startDate && documentDate <= endDate)
                                            {
                                                _logger.LogInformation("Consent Creation Date: {consentCreationDate}", dateElement.ToString());

                                                var destClient = destinationBlobContainerClient.GetBlobClient(blobName);
                                                await destClient.SyncCopyFromUriAsync(new Uri($"{blobClient.Uri}{sasUri.Query}"), cancellationToken: cancellationToken);

                                                Interlocked.Add(ref blobCountMoved, 1);
                                                Interlocked.Add(ref blobBytesMoved, item.Blob.Properties.ContentLength ?? 0);
                                            }
                                        }
                                        catch (JsonException ex)
                                        {
                                            _logger.LogWarning($"Failed to parse JSON in blob: {blobName} - Skipping copy. Error: {ex.Message}");
                                        }

                                        Interlocked.Increment(ref blobCount);
                                        Interlocked.Add(ref blobBytes, item.Blob.Properties.ContentLength ?? 0);
                                    }
                                    finally
                                    {
                                        slim.Release();
                                    }
                                }));
                            }

                        }

                        if (batch.Count > 0)
                        {
                            await SendMessagesInChunksAsync(queueClient, batch, cancellationToken);
                        }
                    });


                    var getDestinationTask = Task.Run(async () =>
                    {
                        await foreach (var item in destinationBlobContainerClient.GetBlobsByHierarchyAsync(prefix: msg.Path, delimiter: _config.Delimiter, cancellationToken: cancellationToken))
                        {
                        
                        }
                    });

                    await Task.WhenAll(getSourceTask, getDestinationTask);


                    if (File.Exists(fileName))
                        File.Delete(fileName);
                        
                    using (StreamWriter sw = new StreamWriter(fileName))
                    {
                        await sw.WriteLineAsync($"File,Source Size,Source MD5,Source Last Modified,Destination Size,Destination MD5,Destination Last Modified");
        
                    }
                    var toUpload = operationBlobContainerClient.GetBlobClient($"{msg.Path}{fileName}");
                    await toUpload.DeleteIfExistsAsync(cancellationToken: cancellationToken);
                    await toUpload.UploadAsync(fileName, cancellationToken: cancellationToken);

                    await Task.WhenAll(blobSet.ToArray());


                    op.Telemetry.Properties.Add("Run", _config.Run);
                    op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());
                    op.Telemetry.Properties.Add("ThreadCount", _config.ThreadCount.ToString());
                    op.Telemetry.Properties.Add("Container", msg.Container);
                    op.Telemetry.Properties.Add("SourceConnection", sourceBlobServiceClient.AccountName);
                    op.Telemetry.Properties.Add("DestinationConnection", destinationBlobServiceClient.AccountName);
                    op.Telemetry.Properties.Add("Delimiter", _config.Delimiter);
                    op.Telemetry.Properties.Add("Prefix", msg.Path);
                    op.Telemetry.Properties.Add("blobCount", blobCount.ToString());
                    op.Telemetry.Properties.Add("blobBytes", blobBytes.ToString());
                    op.Telemetry.Properties.Add("blobCountMoved", blobCountMoved.ToString());
                    op.Telemetry.Properties.Add("blobBytesMoved", blobBytesMoved.ToString());
                    op.Telemetry.Properties.Add("subPrefixes", subPrefixes.ToString());
                }
            }

            using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("Remove Queue Message"))
            {

                op.Telemetry.Properties.Add("Run", _config.Run);
                op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());
                op.Telemetry.Properties.Add("OperationConnection", queueClient.AccountName);
                op.Telemetry.Properties.Add("QueueName", _config.QueueName);

                await queueClient.DeleteMessageAsync(queueMessage.MessageId, queueMessage.PopReceipt, cancellationToken: cancellationToken);
            }

            return true;
        }

        private async Task SendMessagesInChunksAsync(QueueClient queueClient, List<string> messages, CancellationToken cancellationToken)
        {
            const int chunkSize = 32; // Azure Queue Storage limit

            for (int i = 0; i < messages.Count; i += chunkSize)
            {
                var chunk = messages.Skip(i).Take(chunkSize);
                foreach (var msg in chunk)
                {
                    await queueClient.SendMessageAsync(msg, cancellationToken: cancellationToken);
                }
            }
        }

    }
}
