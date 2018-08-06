using System;
using System.IO;
using System.Threading.Tasks;

namespace Synchronizer {
	/// <summary>
	/// Produces Buffers of Members and stores them on Storage, waits when the Storage is full.
	/// </summary>
	public class Producer {
		public Parser.Position Position;
		public int Processed { get; private set; }
		public int Records { get; private set; }
		public int Invalid { get; private set; }
		public event Func<Producer, Exception, Task> OnInvalidItem;
		public event Func<Producer, double, Task> OnProgress;
		public event Func<Producer, Exception, Task> OnFail;
		public int SkipEvery;
		public int StartAt;
		public string ID;

		private Stream stream;
		private Storage<Buffer> storage;
		private readonly int maxDataSize;
		private readonly int batchSize;
		private readonly int bufferSize;

		public Producer(Storage<Buffer> storage, int maxDataSize, int batchSize, int bufferSize) {
			this.storage = storage;
			this.maxDataSize = maxDataSize;
			this.batchSize = batchSize;
			this.bufferSize = bufferSize;
			ID = new Guid().ToString();
		}
		public async Task Start(string path) {
			using (stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
				await Start(stream);
		}
		public async Task Start(Stream stream) {
			try {
				using (var parser = new Parser(this.stream = stream)) {
					if (!storage.Recycle(out var buffer, true))
						throw new Exception("Unable to recycle buffer");
					for (int endMember; (endMember = await parser.Find("/members/member")) != -1 && !storage.IsClosed;) {
						try {
							if (SkipEvery < 1)
								++Records;
							else if ((Records++ + StartAt) % (SkipEvery + 1) != 0)
								continue;
							Position = parser.Current;
							var p = new Member();
							await p.Load(parser, endMember, maxDataSize);
							++Processed;
							p.Validate();
							buffer.Add(p);
							if (buffer.IsFull) {
								storage.Store(buffer);
								OnProgress?.Invoke(this, (double)stream.Position / stream.Length);
								if (!storage.Recycle(out buffer, true))
									throw new Exception("Unable to recycle buffer");
							}
						}
						catch (Exception e) when (e is EPersonInvalid || e is Parser.EMaxLengthExceeded) {
							++Invalid;
							OnInvalidItem?.Invoke(this, e);
						}
					}
					if (buffer.Count > 0)
						storage.Store(buffer);
				}
				OnProgress?.Invoke(this, 1);
			}
			catch (Exception e) {
				OnFail?.Invoke(this, e);
				throw;
			}
		}
	}
}