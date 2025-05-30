
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
                _logger.LogInformation($"ProcessPath New: {msg.Container} {msg.Path}");
                Task.Delay(5000).Wait();
                // using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessPath"))
                // {
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
                    var sourceBlobs = new ConcurrentDictionary<string, BlobInfo>();

                    var destinationBlobServiceClient = new BlobServiceClient(_config.DestinationConnection);
                    var destinationBlobContainerClient = destinationBlobServiceClient.GetBlobContainerClient(msg.Container);
                    var destinationBlobs = new ConcurrentDictionary<string, BlobInfo>();
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
                    var blobs = new Dictionary<string, SourceDestinationInfo>();

                    if (string.IsNullOrEmpty(_config.Delimiter))
                    {
                        _config.Delimiter = "/";
                    }

                    if (_config.ThreadCount < 1)
                    {
                        _config.ThreadCount = Environment.ProcessorCount * 8;
                    }
                    var slim = new SemaphoreSlim(_config.ThreadCount);
                    _logger.LogInformation($"before GetBlobsByHierarchyAsync: {msg.Path} {_config.Delimiter} {cancellationToken}");

                    var getSourceTask = Task.Run(async () =>
                    {
                     _logger.LogInformation($"before GetBlobsByHierarchyAsync: {msg.Path} {_config.Delimiter} {cancellationToken}");
                     var blobItems = sourceBlobContainerClient.GetBlobsAsync(prefix: msg.Path, cancellationToken: cancellationToken);
                     _logger.LogInformation($"blobItems : {blobItems}");
                    await foreach (var blobItem in blobItems)
                    {
                      _logger.LogInformation($"after blobItem : {blobItem}");

                        // Process the blobItem — all items are blobs
                        sourceBlobs.TryAdd(blobItem.Name, new BlobInfo(blobItem.Properties));
                        var subPrefixesDict = new ConcurrentDictionary<string, bool>();

                    
                        await queueClient.SendMessageAsync(new Message()
                        {
                            Action = "ProcessDocument",
                            Container = msg.Container,
                            Path = blobItem.Name
                        }.ToString(), cancellationToken: cancellationToken);
                    
                        // Optional: detect and enqueue sub-prefixes by parsing blob name
                        var remainingPath = blobItem.Name.Substring(msg.Path.Length);
                        var delimiterIndex = remainingPath.IndexOf(_config.Delimiter);
                        if (delimiterIndex > -1)
                        {
                            var subPrefix = msg.Path + remainingPath.Substring(0, delimiterIndex + 1);
                            if (subPrefixesDict.TryAdd(subPrefix, true))
                            {
                                await queueClient.SendMessageAsync(new Message()
                                {
                                    Action = "ProcessPath",
                                    Container = msg.Container,
                                    Path = subPrefix
                                }.ToString(), cancellationToken: cancellationToken);
                                subPrefixes++;
                            }
                        }
                    }
                    });


                    var getDestinationTask = Task.Run(async () =>
                    {
                        await foreach (var item in destinationBlobContainerClient.GetBlobsByHierarchyAsync(prefix: msg.Path, delimiter: _config.Delimiter, cancellationToken: cancellationToken))
                        {
                            Task.Delay(300).Wait();
                            if (item.IsBlob)
                            {
                                destinationBlobs.TryAdd(item.Blob.Name, new BlobInfo(item.Blob.Properties));
                            }
                        }
                    });

                    await Task.WhenAll(getSourceTask, getDestinationTask);


                    if (File.Exists(fileName))
                        File.Delete(fileName);
                        
                    using (StreamWriter sw = new StreamWriter(fileName))
                    {
                        await sw.WriteLineAsync($"File,Source Size,Source MD5,Source Last Modified,Destination Size,Destination MD5,Destination Last Modified");
                        foreach (var item in sourceBlobs)
                        {
                            if (destinationBlobs.ContainsKey(item.Key))
                            {
                                var destinationBlob = destinationBlobs[item.Key];
                                await sw.WriteLineAsync($"{item.Key},{item.Value.Size},{item.Value.ContentMD5},{item.Value.LastModified},{destinationBlob.Size},{destinationBlob.ContentMD5},{destinationBlob.LastModified}");
                                blobs.Add(item.Key, new SourceDestinationInfo(item.Value, destinationBlob));
                            }
                            else
                            {
                                await sw.WriteLineAsync($"{item.Key},{item.Value.Size},{item.Value.ContentMD5},{item.Value.LastModified},,,");
                                blobs.Add(item.Key, new SourceDestinationInfo(item.Value));
                            }
                        }
                    }
                    var toUpload = operationBlobContainerClient.GetBlobClient($"{msg.Path}{fileName}");
                    await toUpload.DeleteIfExistsAsync(cancellationToken: cancellationToken);
                    await toUpload.UploadAsync(fileName, cancellationToken: cancellationToken);

                    var blobSet = new ConcurrentBag<Task>();

                    foreach (var blob in blobs)
                    {
                        blobSet.Add(Task.Run(async () =>
                        {
                            await slim.WaitAsync(cancellationToken);

                            if (blob.Value.Destination == null 
                                || blob.Value.Source.LastModified > blob.Value.Destination.LastModified)
                            {
                                if (!_config.WhatIf)
                                {
                                    var dest = destinationBlobContainerClient.GetBlobClient(blob.Key);
                                    var source = sourceBlobContainerClient.GetBlobClient(blob.Key);

                                    await dest.SyncCopyFromUriAsync(new Uri($"{source.Uri.AbsoluteUri}{sasUri.Query}"));
                                    await source.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, cancellationToken: cancellationToken);

                                }

                                Interlocked.Add(ref blobCountMoved, 1);
                                Interlocked.Add(ref blobBytesMoved, blob.Value.Source.Size);
                            }

                            Interlocked.Add(ref blobCount, 1);
                            Interlocked.Add(ref blobBytes, blob.Value.Source.Size);

                            slim.Release();
                        }));
                    }

                    await Task.WhenAll(blobSet.ToArray());


                    // op.Telemetry.Properties.Add("Run", _config.Run);
                    // op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());
                    // op.Telemetry.Properties.Add("ThreadCount", _config.ThreadCount.ToString());
                    // op.Telemetry.Properties.Add("Container", msg.Container);
                    // op.Telemetry.Properties.Add("SourceConnection", sourceBlobServiceClient.AccountName);
                    // op.Telemetry.Properties.Add("DestinationConnection", destinationBlobServiceClient.AccountName);
                    // op.Telemetry.Properties.Add("Delimiter", _config.Delimiter);
                    // op.Telemetry.Properties.Add("Prefix", msg.Path);
                    // op.Telemetry.Properties.Add("blobCount", blobCount.ToString());
                    // op.Telemetry.Properties.Add("blobBytes", blobBytes.ToString());
                    // op.Telemetry.Properties.Add("blobCountMoved", blobCountMoved.ToString());
                    // op.Telemetry.Properties.Add("blobBytesMoved", blobBytesMoved.ToString());
                    // op.Telemetry.Properties.Add("subPrefixes", subPrefixes.ToString());
                    // }
            }

            else if (msg.Action.Equals("ProcessDocument", StringComparison.InvariantCultureIgnoreCase))
                {
                    _logger.LogInformation($"ProcessDocument: {msg.Container} {msg.Path}");
                    using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessDocument"))
                    {
                        var sourceBlobServiceClient = new BlobServiceClient(_config.SourceConnection);
                        var sourceBlobContainerClient = sourceBlobServiceClient.GetBlobContainerClient(msg.Container);
                        var sourceBlob = sourceBlobContainerClient.GetBlobClient(msg.Path);

                        var destinationBlobServiceClient = new BlobServiceClient(_config.DestinationConnection);
                        var destinationBlobContainerClient = destinationBlobServiceClient.GetBlobContainerClient(msg.Container);
                        await destinationBlobContainerClient.CreateIfNotExistsAsync();

                        var destBlob = destinationBlobContainerClient.GetBlobClient(msg.Path);

                        var operationBlobServiceClient = new BlobServiceClient(_config.OperationConnection);
                        var sasBuilder = new BlobSasBuilder()
                        {
                            BlobContainerName = msg.Container,
                            Resource = "c",
                            ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(_config.VisibilityTimeout)
                        };
                        sasBuilder.SetPermissions(BlobAccountSasPermissions.Read);
                        Uri sasUri = sourceBlobContainerClient.GenerateSasUri(sasBuilder);

                        if (!_config.WhatIf)
                        {
                            await destBlob.SyncCopyFromUriAsync(new Uri($"{sourceBlob.Uri.AbsoluteUri}{sasUri.Query}"));
                            await sourceBlob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots);
                        }

                        op.Telemetry.Properties.Add("BlobPath", msg.Path);
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

    }
}
