using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Synchronizer {
	/// <summary>
	/// Generic concurrent queue, features recycling of items and waiting line. The waiting is done by blocking the origin thread.
	/// </summary>
	public class Storage<T> : IDisposable {
		private ConcurrentQueue<T> wasteBag = new ConcurrentQueue<T>();
		private ConcurrentQueue<T> storeBag = new ConcurrentQueue<T>();
		private ConcurrentQueue<ManualResetEventSlim> wasteListeners = new ConcurrentQueue<ManualResetEventSlim>();
		private ConcurrentQueue<ManualResetEventSlim> storeListeners = new ConcurrentQueue<ManualResetEventSlim>();
		private CancellationTokenSource token = new CancellationTokenSource();

		public bool IsClosed => token.IsCancellationRequested;
		public int WasteCount => wasteBag.Count;
		public int StorageCount => storeBag.Count;
		public void Close() {
			token.Cancel();
			while (storeListeners.TryDequeue(out var l))
				l.Set();
			while (wasteListeners.TryDequeue(out var l))
				l.Set();
		}
		public void Store(T data) {
			storeBag.Enqueue(data);
			if (storeListeners.TryDequeue(out var e))
				e.Set();
		}
		public void Waste(T data) {
			wasteBag.Enqueue(data);
			if (wasteListeners.TryDequeue(out var e))
				e.Set();
		}
		public bool Take(out T data, bool wait = false) {
			bool r;
			while (!(r = storeBag.TryDequeue(out data)) && wait && !IsClosed)
				WaitForStore();
			return r;
		}
		public bool Recycle(out T data, bool wait = false) {
			bool r;
			while (!(r = wasteBag.TryDequeue(out data)) && wait && !IsClosed)
				WaitForWaste();
			return r;
		}
		public void WaitForStore() {
			using (var e = new ManualResetEventSlim(false)) {
				storeListeners.Enqueue(e);
				e.Wait();
			}
		}
		public void WaitForWaste() {
			using (var e = new ManualResetEventSlim(false)) {
				wasteListeners.Enqueue(e);
				e.Wait();
			}
		}
		#region IDisposable Support
		private bool disposedValue = false;
		protected virtual void Dispose(bool disposing) {
			if (!disposedValue) {
				if (disposing)
					Close();
				token.Dispose();
				disposedValue = true;
			}
		}
		public void Dispose() => Dispose(true);
		#endregion		
	}
}