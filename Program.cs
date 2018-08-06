using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Synchronizer {
	class Program {
		static async Task Main(string[] args) {
			Orchestrator orchestrator = null;
			try {
				//Loads options from the arguments
				var options = ReadOptions(args);
				//Creates an orchestrator
				orchestrator = new Orchestrator(new Orchestrator.Settings {
					Buffers = options.To<int>("buffer"),
					BatchSize = options.To<int>("batch"),
					Producers = options.To<int>("p-workers"),
					Consumers = options.To<int>("c-workers"),
					MaxDataLength = options.To<int>("maxlength"),
					UseUpsert = options.To<bool>("upsert"),
					UseIndex = options.To<bool>("index"),
					DatabaseRetriever = () => GetDatabase(options).Result,
					OnInvalid = message => {
						lock (Console.Error)
							Console.Error.WriteLine(message);
					},
					OnLog = message => {
						lock (Console.Error)
							Console.Error.WriteLine(message);
					},
					OnProgress = progress => {
						Console.Title = $"Syncronizer [Progress {(progress * 100).ToString("n2")}%] [Buffer {(orchestrator.BufferLevel * 100).ToString("n2")}%]";
					},
					OnWorkerFail = message => {
						lock (Console.Error)
							Console.Error.WriteLine(message);
					},
				});

				var start = DateTime.Now;
				Console.WriteLine("Timer started");
				//Starts the process
				await orchestrator.Start(options["path"]);

				//Displays the statistics
				using (var process = System.Diagnostics.Process.GetCurrentProcess()) {
					Console.WriteLine("Time elapsed: {0} seconds", DateTime.Now.Subtract(start).TotalSeconds.ToString("n2"));
					Console.WriteLine("Processed records: {0} ({1} invalid records)", orchestrator.Processed, orchestrator.Invalid);
					Console.WriteLine("Records synchronized: {0}", orchestrator.Imported);
					Console.WriteLine("Speed: {0} records/second", (orchestrator.Processed / DateTime.Now.Subtract(start).TotalSeconds).ToString("n2"));
					Console.WriteLine("Peak physical memory: {0} MiB", process.PeakWorkingSet64 >> 20);
					Console.WriteLine("CPU: {0} seconds (user time: {1} seconds)", process.TotalProcessorTime.TotalSeconds.ToString("n2"), process.UserProcessorTime.TotalSeconds.ToString("n2"));
				}
			}
			catch (Exception e) {
				Console.Error.WriteLine("Failed after successfully importing {0} records", orchestrator != null ? orchestrator.Imported : 0);
				var message = new List<string>();
				//Collects exceptions from the workers
				if (e is AggregateException)
					foreach (var ex in (e as AggregateException).InnerExceptions)
						message.Add(ex.Message);
				else
					message.Add(e.Message);
				Console.Error.WriteLine("\nERROR:\n{0}", string.Join("\n", message));
			}
			finally {
				orchestrator?.Dispose();
			}
			Console.Beep();
		}
		static async Task<NpgsqlConnection> GetDatabase(Options opt) {
			var db = new NpgsqlConnection($@"
				Port={opt["port"]};
				Host={opt["host"]};
				Username={opt["user"]};
				Password={opt["pass"]};
				Database={opt["db"]};
				Client Encoding=UTF-8;
				Application Name={AppDomain.CurrentDomain.FriendlyName};
				Command Timeout={opt["timeout"]};
				Timeout={opt["timeout"]}
			");
			await db.OpenAsync();
			return db;
		}
		static Options ReadOptions(string[] args) {
			var opt = new Options {
				{ "host", "localhost" },
				{ "port", "5432", typeof(uint) },
				{ "user", null },
				{ "pass", null },
				{ "db", null },
				{ "path", null },
				{ "batch", "10000", typeof(uint) },
				{ "index", "true", typeof(bool) },
				{ "upsert", "true", typeof(bool) },
				{ "maxlength", "255", typeof(uint) },
				{ "buffer", "4", typeof(uint) },
				{ "timeout", "60", typeof(uint) },
				{ "p-workers", "2", typeof(uint) },
				{ "c-workers", "4", typeof(uint) }
			};
			try {
				opt.Read(args);
				if (opt.To<bool>("upsert") && !opt.To<bool>("index"))
					throw new Exception("upsert = true requires index = true");
				return opt;
			}
			finally {
				Console.WriteLine("INPUT:");
				foreach (var n in opt)
					Console.WriteLine($"\t{n.Key.PadRight(9)}\t\t{n.Value}");
				Console.WriteLine("\n\n");
			}
		}
	}
}