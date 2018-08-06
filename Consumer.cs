using Npgsql;
using System;
using System.Threading.Tasks;

namespace Synchronizer {
	/// <summary>
	/// Consumes Members from the Storage of Buffers while the Storage is opened, pauses when there's no Buffer available.
	/// </summary>
	public class Consumer {
		private NpgsqlConnection db;
		private readonly bool useUpsert;
		private readonly int batchSize;
		private Storage<Buffer> storage;
		public event Func<Consumer, Exception, Task> OnFail;
		public int Imported { get; private set; }
		public string ID;

		public Consumer(Storage<Buffer> storage, NpgsqlConnection db, int batchSize, bool useUpsert) {
			this.storage = storage;
			this.db = db;
			this.batchSize = batchSize;
			this.useUpsert = useUpsert;
			ID = new Guid().ToString();
		}
		public async Task Start() {
			try {
				await Setup();
				using (var update = GetUpdateCommand()) {
					while (!storage.IsClosed || storage.StorageCount > 0) {
						//Nothing to consume, breaks the loop
						if (!storage.Take(out var buffer, true))
							break;
						try {
							using (var transaction = db.BeginTransaction(System.Data.IsolationLevel.ReadCommitted)) {
								var records = buffer.Count;
								using (var writer = db.BeginBinaryImport("COPY synchronize__person (fname, lname, dob, phone) FROM STDIN (FORMAT BINARY)")) {
									for (var i = -1; ++i < buffer.Count;)
										buffer[i].Save(writer);
									writer.Complete();
									writer.Close();
								}
								await update.ExecuteNonQueryAsync();
								await transaction.CommitAsync();
								Imported += records;
								buffer.Clear();
								//Sends the buffer back to be recycled by another Producer
								storage.Waste(buffer);
							}
						}
						catch {
							//Failed to save the buffer, return it to the storage
							storage.Store(buffer);
							throw;
						}
					}
				}
			}
			catch (Exception e) {
				OnFail?.Invoke(this, e);
				throw;
			}
			finally {
				await Teardown();
			}
		}
		public static async Task CreateIndex(NpgsqlConnection db) {
			using (var cmd = db.CreateCommand()) {
				cmd.CommandText = @"
					CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ui_person 
					ON person (fname ASC NULLS LAST, lname ASC NULLS LAST, dob ASC NULLS LAST)
				";
				await cmd.ExecuteNonQueryAsync();
			}
		}
		private async Task Setup() {
			using (var cmd = db.CreateCommand()) {
				cmd.CommandText = $@"
					SET datestyle = 'ISO, YMD';
					SET temp_buffers = '{(int)Math.Ceiling((double)batchSize / 500)}MB';
					CREATE TEMP TABLE IF NOT EXISTS synchronize__person
					AS SELECT * FROM person LIMIT 0;
				";
				await cmd.ExecuteNonQueryAsync();
			}
		}
		private async Task Teardown() {
			using (var cmd = db.CreateCommand()) {
				cmd.CommandText = "DROP TABLE IF EXISTS synchronize__person";
				await cmd.ExecuteNonQueryAsync();
			}
		}
		private NpgsqlCommand GetUpdateCommand() {
			var cmd = db.CreateCommand();
			if (useUpsert) {
				cmd.CommandText = @"
					INSERT INTO person (fname, lname, dob, phone)
					SELECT fname, lname, dob, phone
					FROM synchronize__person
					ON CONFLICT (fname, lname, dob)
					DO UPDATE
						SET phone = excluded.phone;
					TRUNCATE TABLE synchronize__person
				";
			}
			else
				cmd.CommandText = @"
					UPDATE person p
					SET phone = r.phone
					FROM synchronize__person r
					WHERE 
						(
							p.fname = r.fname
							OR p.fname IS NULL AND r.fname IS NULL
						)
						AND (
							p.lname = r.lname
							OR (p.lname IS NULL AND r.lname IS NULL)
						)
						AND (
							p.dob = r.dob
							OR p.dob IS NULL AND r.dob IS NULL
						)
						AND (
							p.phone <> r.phone
							OR r.phone IS NULL 
							OR p.phone IS NULL
						);
					INSERT INTO person (fname, lname, dob, phone)
					SELECT r.fname, r.lname, r.dob, r.phone
					FROM synchronize__person r
					WHERE NOT EXISTS(
						SELECT 0 
						FROM person p
						WHERE 
							(
								p.fname = r.fname
								OR p.fname IS NULL AND r.fname IS NULL
							)
							AND (
								p.lname = r.lname
								OR (p.lname IS NULL AND r.lname IS NULL)
							)
							AND (
								p.dob = r.dob
								OR p.dob IS NULL AND r.dob IS NULL
							)
					);
					TRUNCATE TABLE synchronize__person
				";
			return cmd;
		}
	}
}