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
    List<Message> messageBatch = new List<Message>();  // Batch for messages
    const int maxBatchSize = 10000;  // Max number of messages to push in a single batch

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

    // Processing the "ProcessAccount" action
    if (msg.Action.Equals("ProcessAccount", StringComparison.InvariantCultureIgnoreCase))
    {
        _logger.LogInformation("ProcessAccount");

        using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessAccount"))
        {
            var sourceBlobServiceClient = new BlobServiceClient(_config.SourceConnection);
            int containerCount = 0;

            foreach (var container in sourceBlobServiceClient.GetBlobContainers())
            {
                // Create a message for each container and add to the batch
                var message = new Message()
                {
                    Action = "ProcessPath",
                    Container = container.Name,
                    Path = string.Empty
                };
                messageBatch.Add(message);
                containerCount++;

                // If batch is full, send the messages and clear the batch
                if (messageBatch.Count >= maxBatchSize)
                {
                    await SendBatchMessages(queueClient, messageBatch, cancellationToken);
                    messageBatch.Clear();  // Clear the batch after sending
                }
            }

            // Send remaining messages if any
            if (messageBatch.Any())
            {
                await SendBatchMessages(queueClient, messageBatch, cancellationToken);
            }

            op.Telemetry.Properties.Add("Run", _config.Run);
            op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());
            op.Telemetry.Properties.Add("SourceConnection", sourceBlobServiceClient.AccountName);
            op.Telemetry.Properties.Add("containerCount", containerCount.ToString());
        }
    }

    // Processing other actions such as "ProcessPath" and "ProcessDocument"
    else if (msg.Action.Equals("ProcessPath", StringComparison.InvariantCultureIgnoreCase))
    {
        _logger.LogInformation($"ProcessPath: {msg.Container} {msg.Path}");

        // Process paths, collect messages, and add them to the batch
        using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessPath"))
        {
            var sourceBlobServiceClient = new BlobServiceClient(_config.SourceConnection);
            var sourceBlobContainerClient = sourceBlobServiceClient.GetBlobContainerClient(msg.Container);

            await foreach (var item in sourceBlobContainerClient.GetBlobsByHierarchyAsync(prefix: msg.Path, delimiter: _config.Delimiter, cancellationToken: cancellationToken))
            {
                if (item.IsPrefix)
                {
                    // Create a new message for the prefix and add it to the batch
                    var prefixMessage = new Message()
                    {
                        Action = "ProcessPath",
                        Container = msg.Container,
                        Path = item.Prefix
                    };
                    messageBatch.Add(prefixMessage);

                    // If batch is full, send the messages and clear the batch
                    if (messageBatch.Count >= maxBatchSize)
                    {
                        await SendBatchMessages(queueClient, messageBatch, cancellationToken);
                        messageBatch.Clear();
                    }
                }
                else if (item.IsBlob)
                {
                    // Create a message for the blob and add to the batch
                    var blobMessage = new Message()
                    {
                        Action = "ProcessDocument",
                        Container = msg.Container,
                        Path = item.Blob.Name
                    };
                    messageBatch.Add(blobMessage);

                    // If batch is full, send the messages and clear the batch
                    if (messageBatch.Count >= maxBatchSize)
                    {
                        await SendBatchMessages(queueClient, messageBatch, cancellationToken);
                        messageBatch.Clear();
                    }
                }
            }

            // Send remaining messages if any
            if (messageBatch.Any())
            {
                await SendBatchMessages(queueClient, messageBatch, cancellationToken);
            }
        }
    }

    else if (msg.Action.Equals("ProcessDocument", StringComparison.InvariantCultureIgnoreCase))
    {
        _logger.LogInformation($"ProcessDocument: {msg.Container} {msg.Path}");

        // Process document and add additional messages if needed
        using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessDocument"))
        {
            var sourceBlobServiceClient = new BlobServiceClient(_config.SourceConnection);
            var sourceBlobContainerClient = sourceBlobServiceClient.GetBlobContainerClient(msg.Container);
            var sourceBlob = sourceBlobContainerClient.GetBlobClient(msg.Path);

            var destinationBlobServiceClient = new BlobServiceClient(_config.DestinationConnection);
            var destinationBlobContainerClient = destinationBlobServiceClient.GetBlobContainerClient(msg.Container);
            await destinationBlobContainerClient.CreateIfNotExistsAsync();

            var destBlob = destinationBlobContainerClient.GetBlobClient(msg.Path);

            if (!_config.WhatIf)
            {
                await destBlob.SyncCopyFromUriAsync(new Uri($"{sourceBlob.Uri.AbsoluteUri}{sasUri.Query}"));
                await sourceBlob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
            }

            op.Telemetry.Properties.Add("BlobPath", msg.Path);
        }
    }

    // Remove queue message after processing
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

private async Task SendBatchMessages(QueueClient queueClient, List<Message> messageBatch, CancellationToken cancellationToken)
{
    if (messageBatch.Any())
    {
        // Convert List<Message> to an array of string messages
        var batchMessages = messageBatch.Select(m => m.ToString()).ToArray();

        // Send messages in the batch to the queue
        await queueClient.SendMessagesAsync(batchMessages, cancellationToken);
        _logger.LogInformation($"Batch of {batchMessages.Length} messages sent to the queue.");
    }
}


    }
}
