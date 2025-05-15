protected async Task<bool> ProcessQueueMessage(CancellationToken cancellationToken)
{
    // Create a QueueClient to interact with the queue
    var queueClient = new QueueClient(_config.OperationConnection, _config.QueueName);
    QueueMessage queueMessage = null;
    Message msg = null;

    using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("GetMessage"))
    {
        // Telemetry for Queue operation
        op.Telemetry.Properties.Add("Run", _config.Run);
        op.Telemetry.Properties.Add("WhatIf", _config.WhatIf.ToString());
        op.Telemetry.Properties.Add("OperationConnection", queueClient.AccountName);
        op.Telemetry.Properties.Add("QueueName", _config.QueueName);
        op.Telemetry.Properties.Add("VisibilityTimeout", _config.VisibilityTimeout.ToString());

        // Check if the queue exists, if not, create it
        await queueClient.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

        if (queueClient.Exists())
        {
            // Retrieve a message from the queue
            queueMessage = await queueClient.ReceiveMessageAsync(TimeSpan.FromMinutes(_config.VisibilityTimeout), cancellationToken);

            if (queueMessage != null)
            {
                // Convert the message text to a Message object
                msg = Message.FromString(queueMessage.MessageText);
            }
        }

        // If no message, exit the method
        if (msg == null)
        {
            return false;
        }
    }

    // If the message action is "ProcessPath", handle blob processing
    if (msg.Action.Equals("ProcessPath", StringComparison.InvariantCultureIgnoreCase))
    {
        _logger.LogInformation($"Processing path: {msg.Container} {msg.Path}");
        using (var op = _telemetryClient.StartOperation<DependencyTelemetry>("ProcessPath"))
        {
            var sourceBlobServiceClient = new BlobServiceClient(_config.SourceConnection);
            var sourceBlobContainerClient = sourceBlobServiceClient.GetBlobContainerClient(msg.Container);

            // Create SAS URI to access the blobs
            var sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = msg.Container,
                Resource = "c",
                ExpiresOn = DateTimeOffset.UtcNow.AddMinutes(_config.VisibilityTimeout)
            };
            sasBuilder.SetPermissions(BlobAccountSasPermissions.Read);
            Uri sasUri = sourceBlobContainerClient.GenerateSasUri(sasBuilder);

            // Create a destination BlobServiceClient and containers if not exist
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

            // Iterate through blobs and create a message for each blob (document)
            await foreach (var item in sourceBlobContainerClient.GetBlobsByHierarchyAsync(prefix: msg.Path, delimiter: _config.Delimiter, cancellationToken: cancellationToken))
            {
                if (item.IsBlob)
                {
                    // For each blob, create a new message to send to the queue
                    var blobName = item.Blob.Name;
                    var blobClient = sourceBlobContainerClient.GetBlobClient(blobName);

                    // Add a task to copy each blob and send a message to the queue
                    blobSet.Add(Task.Run(async () =>
                    {
                        await slim.WaitAsync(cancellationToken);
                        try
                        {
                            // Download blob content
                            var downloadResponse = await blobClient.DownloadAsync(cancellationToken: cancellationToken);
                            using var reader = new StreamReader(downloadResponse.Value.Content);
                            var content = await reader.ReadToEndAsync();

                            try
                            {
                                // Parse JSON (if the blob content is JSON)
                                var json = JsonDocument.Parse(content).RootElement;

                                var startDate = DateTime.UtcNow.Date.AddDays(-30);
                                var endDate = DateTime.UtcNow.Date.AddDays(1).AddTicks(-1);

                                if (json.TryGetProperty("consentCreationDate", out var dateElement) &&
                                    json.TryGetProperty("isAnonymous", out var isAnonElement) &&
                                    isAnonElement.ValueKind == JsonValueKind.False &&
                                    dateElement.TryGetDateTime(out var documentDate) &&
                                    documentDate >= startDate && documentDate <= endDate)
                                {
                                    // Send message for each document (blob) that passes the criteria
                                    _logger.LogInformation("Sending message for document: {blobName}", blobName);
                                    var messageText = new Message()
                                    {
                                        Action = "ProcessDocument",
                                        Container = msg.Container,
                                        BlobName = blobName
                                    }.ToString();

                                    await queueClient.SendMessageAsync(messageText, cancellationToken: cancellationToken);

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

            // Wait for all blob processing tasks to complete
            await Task.WhenAll(blobSet.ToArray());

            // After processing all blobs, log the results
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

    // Delete the processed message from the queue
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
