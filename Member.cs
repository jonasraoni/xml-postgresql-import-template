using Npgsql;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Synchronizer {
	/// <summary>
	/// Loads, stores and validates members, throws on invalid items.
	/// </summary>
	public class Member : Dictionary<string, string> {
		private static readonly List<Parser.Query> fields = new List<Parser.Query> { "firstname", "lastname", "date-of-birth", "phone" };
		public async Task Load(Parser parser, int endMember, int maxDataSize) {
			while (await parser.Find(fields, endMember) != -1)
				this[parser.TagName] = (await parser.Read(maxDataSize)).ToString().Trim();
		}
		public void Save(NpgsqlBinaryImporter writer) {
			writer.StartRow();
			writer.Write(this["firstname"], NpgsqlTypes.NpgsqlDbType.Varchar);
			writer.Write(this["lastname"], NpgsqlTypes.NpgsqlDbType.Varchar);
			writer.Write(DateTime.Parse(this["date-of-birth"]), NpgsqlTypes.NpgsqlDbType.Date);
			writer.Write(this["phone"], NpgsqlTypes.NpgsqlDbType.Varchar);
		}
		public void Validate() {
			foreach (var n in new[] { "firstname", "lastname", "date-of-birth", "phone" })
				if (!ContainsKey(n))
					throw new EPersonInvalid($"{n} is required");
			if (this["phone"]?.Length > 10)
				throw new EPersonInvalid($"phone can't have more than 10 characters");
			if (this["date-of-birth"]?.Length > 0)
				try {
					DateTime.Parse(this["date-of-birth"]);
				}
				catch {
					throw new EPersonInvalid($"date-of-birth \"{this["date-of-birth"]}\" is invalid");
				}
		}
		public new string this[string index] {
			get => base[index];
			set => base[index] = string.IsNullOrEmpty(value) ? null : value;
		}
	}

	public class EPersonInvalid : Exception {
		public EPersonInvalid(string message) : base(message) {
		}
	}
}