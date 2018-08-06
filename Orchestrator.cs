using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Synchronizer {
	/// <summary>
	/// Orchestrates the Producer <=> Storage <=> Consumer communication between the workers and provides feedback.
	/// </summary>
	public class Orchestrator : IDisposable {
		private Storage<Buffer> storage;
		private List<Consumer> consumers = new List<Consumer>();
		private List<Producer> producers = new List<Producer>();
		private Settings settings;
		private List<NpgsqlConnection> databases = new List<NpgsqlConnection>();

		public Orchestrator(Settings settings) {
			this.settings = settings;
			storage = new Storage<Buffer>();
			for (var i = settings.Buffers; i-- > 0;)
				storage.Waste(new Buffer(settings.BatchSize));
			CreateProducers();
			CreateConsumers();
			if (settings.UseIndex)
				CreateIndex().Wait();
		}
		public async Task Start(string path) {
			List<Task> pTasks = new List<Task>(), cTasks = new List<Task>();
			Task producerTask = null, consumerTask = null;
			var start = DateTime.Now;
			//Attempts to create producers and consumers in a 1/1 ratio.
			for (var i = Math.Max(settings.Producers, settings.Consumers); i-- > 0;) {
				if (i < producers.Count)
					pTasks.Add(producers[i].Start(path));
				if (i < consumers.Count)
					cTasks.Add(consumers[i].Start());
			}
			//Collects all exceptions
			var exceptions = new List<Exception>();
			try {
				await (producerTask = Task.WhenAll(pTasks));
			}
			catch {
				if (producerTask.Exception != null)
					exceptions.AddRange(producerTask.Exception.InnerExceptions);
			}
			//When all the producers are done, closes the storage, which will terminate the consumers gracefully.
			storage.Close();
			try {
				await (consumerTask = Task.WhenAll(cTasks));
			}
			catch {
				if (consumerTask.Exception != null)
					exceptions.AddRange(consumerTask.Exception.InnerExceptions);
			}
			if (exceptions.Count > 0)
				throw new AggregateException(exceptions);
		}
		private void CreateConsumers() {
			for (var i = -1; ++i < settings.Consumers;) {
				settings.OnLog?.Invoke($"Creating consumer #{i}");
				var db = settings.DatabaseRetriever();
				databases.Add(db);
				var c = new Consumer(storage, db, settings.BatchSize, settings.UseUpsert) {
					ID = $"#{i}",
				};
				c.OnFail += async (o, e) => {
					await Task.Run(() => {
						settings.OnWorkerFail?.Invoke($"Consumer {o.ID} died {e.Message}");
					});
				};
				consumers.Add(c);
			}
		}
		public void CreateProducers() {
			var progressList = new double[settings.Producers];
			for (var i = -1; ++i < settings.Producers;) {
				settings.OnLog?.Invoke($"Creating producer #{i}");
				var p = new Producer(storage, settings.MaxDataLength, settings.BatchSize, settings.Buffers) {
					SkipEvery = settings.Producers > 1 ? settings.Producers - 1 : 0,
					StartAt = i,
					ID = $"#{i}"
				};
				p.OnInvalidItem += async (o, e) => {
					await Task.Run(() => {
						settings.OnInvalid?.Invoke($"Invalid member at {o.Position.Line}:{o.Position.Column} {e.Message}");
					});
				};
				p.OnFail += async (o, e) => {
					await Task.Run(() => {
						settings.OnWorkerFail?.Invoke($"Producer {o.ID} died at {o.Position.Line}:{o.Position.Column} {e.Message}");
					});
				};
				p.OnProgress += async (o, progress) => {
					await Task.Run(() => {
						progressList[o.StartAt] = progress;
						double t = 0;
						Array.ForEach(progressList, item => t += item);
						settings.OnProgress?.Invoke(t / producers.Count);
					});
				};
				producers.Add(p);
			}
		}
		public int Imported {
			get {
				var imported = 0;
				consumers.ForEach(o => imported += o.Imported);
				return imported;
			}
		}
		public int Processed {
			get {
				var processed = 0;
				producers.ForEach(o => processed += o.Processed);
				return processed;
			}
		}
		public int Invalid {
			get {
				var invalid = 0;
				producers.ForEach(o => invalid += o.Invalid);
				return invalid;
			}
		}
		public double BufferLevel => (double)storage.StorageCount / settings.Buffers;
		private async Task CreateIndex() {
			using (var db = settings.DatabaseRetriever()) {
				settings.OnLog?.Invoke("Attempting to create unique index \"ui_person\" on table \"person\"");
				await Consumer.CreateIndex(db);
				settings.OnLog?.Invoke("Index created");
			}
		}

		#region IDisposable Support
		private bool disposedValue = false;
		protected virtual void Dispose(bool disposing) {
			if (!disposedValue) {
				if (disposing) {
					storage.Dispose();
					databases.ForEach(db => {
						try {
							db.Dispose();
						}
						catch {
						}
					});
				}
				disposedValue = true;
			}
		}
		public void Dispose() => Dispose(true);
		#endregion

		public class Settings {
			public int BatchSize;
			public bool UseUpsert;
			public int Buffers;
			public int Consumers;
			public int Producers;
			public int MaxDataLength;
			public Func<NpgsqlConnection> DatabaseRetriever;
			public bool UseIndex;
			public Action<string> OnLog;
			public Action<string> OnWorkerFail;
			public Action<string> OnInvalid;
			public Action<double> OnProgress;
		}
	}
}