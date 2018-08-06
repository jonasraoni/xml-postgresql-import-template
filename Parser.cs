using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Synchronizer {
	/// <summary>
	/// Asynchronous SAX parser with some XPath queries.
	/// </summary>
	public class Parser : IDisposable {
		private const int BUFFER_SIZE = 4096;
		private readonly XmlReader reader;
		private readonly List<string> path = new List<string>();
		private readonly char[] buffer = new char[BUFFER_SIZE];

		public Parser(Stream stream) {
			reader = XmlReader.Create(stream, new XmlReaderSettings {
				Async = true,
				IgnoreComments = true,
				IgnoreProcessingInstructions = true,
				IgnoreWhitespace = true
			});
		}
		public async Task<int> Find(IEnumerable<Query> query, int stopDepth = 0) {
			while (await reader.ReadAsync()) {
				switch (reader.NodeType) {
					case XmlNodeType.Element:
						path.Add(reader.Name);
						bool found = false;
						foreach (var q in query)
							if (q.IsMatch(path)) {
								found = true;
								break;
							}
						if (reader.IsEmptyElement)
							path.RemoveAt(path.Count - 1);
						if (found)
							return reader.Depth;
						break;
					case XmlNodeType.EndElement:
						path.RemoveAt(path.Count - 1);
						if (stopDepth == reader.Depth)
							return -1;
						break;
				}
			}
			return -1;
		}
		public async Task<int> Find(string query, int stopDepth = 0) => await Find(new List<Query> { new Query(query) }, stopDepth);
		public async Task<StringBuilder> Read(int? maxLength = null) {
			var value = new StringBuilder();
			var depth = reader.Depth;
			while (await reader.ReadAsync()) {
				switch (reader.NodeType) {
					case XmlNodeType.SignificantWhitespace:
					case XmlNodeType.CDATA:
					case XmlNodeType.Text:
					case XmlNodeType.Whitespace:
						var read = 0;
						do {
							if ((read = await reader.ReadValueChunkAsync(buffer, 0, BUFFER_SIZE)) > 0) {
								if (maxLength != null && value.Length + read > maxLength)
									throw new EMaxLengthExceeded(maxLength.Value);
								value.Append(buffer, 0, read);
							}
						}
						while (read == BUFFER_SIZE);
						break;
					case XmlNodeType.EndElement:
						if (reader.Depth == depth) {
							path.RemoveAt(path.Count - 1);
							return value;
						}
						break;
				}
			}
			return value;
		}
		public string TagName => reader.Name;
		public Position Current => new Position((reader as IXmlLineInfo).LineNumber, (reader as IXmlLineInfo).LinePosition);
		public struct Position {
			public int Line;
			public int Column;
			public Position(int line, int column) {
				Line = line;
				Column = column;
			}
		}

		public class EMaxLengthExceeded : Exception {
			public EMaxLengthExceeded(int maxLength) : base($"Limit of {maxLength} bytes exceeded") {
			}
		}

		public class Query {
			private readonly bool forward;
			private string[] pieces;

			public Query(string query) {
				pieces = query.Split('/');
				forward = pieces[0] == "";
			}
			public bool IsMatch(List<string> tags) {
				if (forward) {
					if (pieces.Length == tags.Count + 1) {
						for (var i = pieces.Length; --i > 0;)
							if (pieces[i] != tags[i - 1])
								return false;
						return true;
					}
				}
				else if (pieces.Length <= tags.Count) {
					for (var i = pieces.Length; i-- > 0;)
						if (pieces[i] != tags[tags.Count - (pieces.Length - i)])
							return false;
					return true;
				}
				return false;
			}
			public static implicit operator Query(string query) => new Query(query);
		}

		#region IDisposable Support
		private bool disposedValue = false;

		protected virtual void Dispose(bool disposing) {
			if (!disposedValue) {
				if (disposing)
					reader.Dispose();
				disposedValue = true;
			}
		}
		public void Dispose() => Dispose(true);
		#endregion
	}
}