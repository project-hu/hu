// ==============================================================================================================
// Microsoft patterns & practices
// CQRS Journey project
// ==============================================================================================================
// ©2012 Microsoft. All rights reserved. Certain content used with permission from contributors
// http://go.microsoft.com/fwlink/p/?LinkID=258575
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance 
// with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License is 
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
// See the License for the specific language governing permissions and limitations under the License.
// ==============================================================================================================

using Microsoft.WindowsAzure.Storage.Table.Queryable;

namespace Infrastructure.Azure.EventSourcing
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.Services.Client;
    using System.Diagnostics;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using AutoMapper;
    using Microsoft.Practices.EnterpriseLibrary.WindowsAzure.TransientFaultHandling.AzureStorage;
    using Microsoft.Practices.TransientFaultHandling;
  //  using Microsoft.WindowsAzure;
    //using Microsoft.WindowsAzure.StorageClient;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    /// <summary>
    /// Implements an event store using Windows Azure Table Storage.
    /// </summary>
    /// <remarks>
    /// <para> This class works closely related to <see cref="EventStoreBusPublisher"/> and <see cref="AzureEventSourcedRepository{T}"/>, and provides a resilient mechanism to 
    /// store events, and also manage which events are pending for publishing to an event bus.</para>
    /// <para>Ideally, it would be very valuable to provide asynchronous APIs to avoid blocking I/O calls.</para>
    /// <para>See <see cref="http://go.microsoft.com/fwlink/p/?LinkID=258557"> Journey chapter 7</see> for more potential performance and scalability optimizations.</para>
    /// </remarks>
    public class EventStore : IEventStore, IPendingEventsQueue
    {
        private const string UnpublishedRowKeyPrefix = "Unpublished_";
        private const string UnpublishedRowKeyPrefixUpperLimit = "Unpublished`";
        private const string RowKeyVersionUpperLimit = "9999999999";
        private readonly CloudStorageAccount account;
        private readonly string tableName;
        private readonly CloudTableClient tableClient;
        private readonly Microsoft.Practices.TransientFaultHandling.RetryPolicy pendingEventsQueueRetryPolicy;
        private readonly Microsoft.Practices.TransientFaultHandling.RetryPolicy eventStoreRetryPolicy;

        static EventStore()
        {
            Mapper.CreateMap<EventTableServiceEntity, EventData>();
            Mapper.CreateMap<EventData, EventTableServiceEntity>();
        }

        public EventStore(CloudStorageAccount account, string tableName)
        {
            if (account == null) throw new ArgumentNullException("account");
            if (tableName == null) throw new ArgumentNullException("tableName");
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentException("tableName");

            this.account = account;
            this.tableName = tableName;
            this.tableClient = account.CreateCloudTableClient();
#pragma warning disable 618
            //this.tableClient.RetryPolicy = RetryPolicies.NoRetry();
#pragma warning restore 618

            // TODO: This could be injected.
            var backgroundRetryStrategy = new ExponentialBackoff(10, TimeSpan.FromMilliseconds(100),
                TimeSpan.FromSeconds(15), TimeSpan.FromSeconds(1));
            var blockingRetryStrategy = new Incremental(3, TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
            this.pendingEventsQueueRetryPolicy =
                new RetryPolicy<StorageTransientErrorDetectionStrategy>(backgroundRetryStrategy);
            this.pendingEventsQueueRetryPolicy.Retrying += (s, e) =>
            {
                var handler = this.Retrying;
                if (handler != null)
                {
                    handler(this, EventArgs.Empty);
                }

                Trace.TraceWarning(
                    "An error occurred in attempt number {1} to access table storage (PendingEventsQueue): {0}",
                    e.LastException.Message, e.CurrentRetryCount);
            };
            this.eventStoreRetryPolicy = new RetryPolicy<StorageTransientErrorDetectionStrategy>(blockingRetryStrategy);
            this.eventStoreRetryPolicy.Retrying += (s, e) => Trace.TraceWarning(
                "An error occurred in attempt number {1} to access table storage (EventStore): {0}",
                e.LastException.Message,
                e.CurrentRetryCount);
            //Changed as Part of Code Migration
            //Previous Code:tableClient.CreateTableIfNotExist(tableName));
            var cloudTable = tableClient.GetTableReference(tableName);
            this.eventStoreRetryPolicy.ExecuteAction(() => cloudTable.CreateIfNotExists());

            // cloudTable.CreateIfNotExists()


        }

        /// <summary>
        /// Notifies that the sender is retrying due to a transient fault.
        /// </summary>
        public event EventHandler Retrying;

        public IEnumerable<EventData> Load(string partitionKey, int version)
        {
            var minRowKey = version.ToString("D10");
            var query = this.GetEntitiesQuery(partitionKey, minRowKey, RowKeyVersionUpperLimit);
            // TODO: use async APIs, continuation tokens
            var all = this.eventStoreRetryPolicy.ExecuteAction(() => query.Execute());
            return all.Select(x => Mapper.Map(x, new EventData {Version = int.Parse(x.RowKey)}));
        }

        public void Save(string partitionKey, IEnumerable<EventData> events)
        {
            TableBatchOperation batchOperation = new TableBatchOperation();
            var context = this.tableClient.GetTableReference(this.tableName);
            foreach (var eventData in events)
            {
                string creationDate = DateTime.UtcNow.ToString("o");
                var formattedVersion = eventData.Version.ToString("D10");
            
                batchOperation.Insert(Mapper.Map(eventData, new EventTableServiceEntity
                    {
                        PartitionKey = partitionKey,
                        RowKey = formattedVersion,
                        CreationDate = creationDate,
                    }));

             //Need to log the Insert Operation.


                // Add a duplicate of this event to the Unpublished "queue"
                batchOperation.Insert(
                    Mapper.Map(eventData, new EventTableServiceEntity
                    {
                        PartitionKey = partitionKey,
                        RowKey = UnpublishedRowKeyPrefix + formattedVersion,
                        CreationDate = creationDate,
                    }));
            }

            //Need to log the Batch Operation


            try
            {
                this.eventStoreRetryPolicy.ExecuteAction(() => context.ExecuteBatch(batchOperation));
            }
            catch (StorageException  ex)
            {



                var requestInformation = ex.RequestInformation;

                Trace.WriteLine(requestInformation.HttpStatusMessage);

                // get more details about the exception
                var information = requestInformation.ExtendedErrorInformation;

                // if you have aditional information, you can use it for your logs
                if (information == null)
                    throw;

                var errorCode = information.ErrorCode;

                if (errorCode != null && errorCode == "EntityAlreadyExists")
                {
                    throw new ConcurrencyException();
                }

                throw;
            }
        }

        /// <summary>
        /// Gets the pending events for publishing asynchronously using delegate continuations.
        /// </summary>
        /// <param name="partitionKey">The partition key to get events from.</param>
        /// <param name="successCallback">The callback that will be called if the data is successfully retrieved. 
        /// The first argument of the callback is the list of pending events.
        /// The second argument is true if there are more records that were not retrieved.</param>
        /// <param name="exceptionCallback">The callback used if there is an exception that does not allow to continue.</param>
        public void GetPendingAsync(string partitionKey, Action<IEnumerable<IEventRecord>, bool> successCallback,
            Action<Exception> exceptionCallback)
        {

            var query = this.GetEntitiesQuery(partitionKey, UnpublishedRowKeyPrefix, UnpublishedRowKeyPrefixUpperLimit);
            var token = new TableContinuationToken();
            this.pendingEventsQueueRetryPolicy
                .ExecuteAction(
                    ac => query.BeginExecuteSegmented(token,ac, null),
                    ar => query.EndExecuteSegmented(ar),
                    rs =>
                    {
                        var all = rs.Results.ToList();
                        successCallback(rs.Results,rs.ContinuationToken!=null);
                    },
                    exceptionCallback);


        }

        /// <summary>
        /// Deletes the specified pending event from the queue.
        /// </summary>
        /// <param name="partitionKey">The partition key of the event.</param>
        /// <param name="rowKey">The partition key of the event.</param>
        /// <param name="successCallback">The callback that will be called if the data is successfully retrieved.
        /// The argument specifies if the row was deleted. If false, it means that the row did not exist.
        /// </param>
        /// <param name="exceptionCallback">The callback used if there is an exception that does not allow to continue.</param>
        public void DeletePendingAsync(string partitionKey, string rowKey, Action<bool> successCallback,
            Action<Exception> exceptionCallback)
        {

            try
            {
                var context = this.tableClient.GetTableReference(this.tableName);
                var item = new EventTableServiceEntity { PartitionKey = partitionKey, RowKey = rowKey,ETag="*" };
                TableOperation deleteOperation = TableOperation.Delete(item);
              
                this.pendingEventsQueueRetryPolicy.ExecuteAction(
                    ac => context.BeginExecute(deleteOperation, ac, null),
                    ar =>
                    {
                        try
                        {
                            context.EndExecute(ar);
                            return true;
                        }
                        catch (StorageException ex)
                        {



                            var requestInformation = ex.RequestInformation;

                            Trace.WriteLine(requestInformation.HttpStatusMessage);

                            // get more details about the exception
                            var information = requestInformation.ExtendedErrorInformation;

                            // if you have aditional information, you can use it for your logs
                            if (information == null)
                                throw;

                            var errorCode = information.ErrorCode;

                            if (errorCode != null && errorCode == "Not Found ")
                            {
                                throw;
                            }

                            return false;
                        }
                    },
                    successCallback,
                    exceptionCallback);

            }
            catch (Exception ex)
            {
                string msg = ex.Message;

                throw;
            }
          
        
        }

        /// <summary>
        /// Gets the list of all partitions that have pending unpublished events.
        /// </summary>
        /// <returns>The list of all partitions.</returns>
        public IEnumerable<string> GetPartitionsWithPendingEvents()
        {
            
      

            CloudTable table = this.tableClient.GetTableReference(this.tableName);

            var query = TableQueryableExtensions.AsTableQuery(table.CreateQuery<EventTableServiceEntity>()
                .Where(eventTable => System.String.Compare(eventTable.RowKey, UnpublishedRowKeyPrefix,
                    System.StringComparison.Ordinal) >= 0 &&
                                                                   System.String.Compare(eventTable.RowKey, UnpublishedRowKeyPrefixUpperLimit,
                                                                       System.StringComparison.Ordinal) <= 0).Select(x => new { x.PartitionKey }));




            var token = new TableContinuationToken();
            var result = new BlockingCollection<string>();
            var tokenSource = new CancellationTokenSource();

            this.pendingEventsQueueRetryPolicy.ExecuteAction(
                ac => query.BeginExecuteSegmented(token, ac, null),
                ar => query.EndExecuteSegmented(ar),
                rs =>
                {
                    foreach (var key in rs.Results.Select(x => x.PartitionKey).Distinct())
                    {
                        result.Add(key);
                    }

                    while (rs.ContinuationToken != null)
                    {
                        try
                        {

                            rs = this.pendingEventsQueueRetryPolicy.ExecuteAction(() => query.ExecuteSegmented(token));
                            foreach (var key in rs.Results.Select(x => x.PartitionKey).Distinct())
                            {
                                result.Add(key);
                            }
                        }
                        catch
                        {
                            // Cancel is to force an exception being thrown in the consuming enumeration thread
                            // TODO: is there a better way to get the correct exception message instead of an OperationCancelledException in the consuming thread?
                            tokenSource.Cancel();
                            throw;
                        }
                    }
                    result.CompleteAdding();
                },
                ex =>
                {
                    tokenSource.Cancel();
                    throw ex;
                });

            return result.GetConsumingEnumerable(tokenSource.Token);
         
        }

        private TableQuery<EventTableServiceEntity> GetEntitiesQuery(string partitionKey, string minRowKey,
            string maxRowKey)
        {

            CloudTable table = this.tableClient.GetTableReference(this.tableName);

            var entityQuery = table.CreateQuery<EventTableServiceEntity>()
                .Where(eventTable =>eventTable.PartitionKey == partitionKey
                                                                  &&
                                                                 System.String.Compare(eventTable.RowKey, minRowKey,
                                                                    System.StringComparison.Ordinal) >= 0 
                &&
                                                                   System.String.Compare(eventTable.RowKey, maxRowKey,
                                                                       System.StringComparison.Ordinal) <= 0).AsTableQuery<EventTableServiceEntity>();



            return entityQuery;
        }
    }
}
