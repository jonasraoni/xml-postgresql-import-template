using System;
using System.Collections.Generic;

namespace Synchronizer {
	/// <summary>
	/// A simple class to parse options from the arguments, throws on invalid/missing arguments.
	/// </summary>
	public class Options : Dictionary<string, string> {
		private readonly Dictionary<string, Type> map = new Dictionary<string, Type>();
		public void Add(string name, string value, Type type = null) {
			this[name] = value;
			map[name] = type ?? typeof(string);
		}
		public Options Read(params string[] args) {
			string name = null;
			foreach (var v in args)
				if (name == null) {
					if (ContainsKey(v))
						name = v;
				}
				else {
					try {
						this[name] = Convert.ChangeType(v, map[name]).ToString();
					}
					catch (Exception e) {
						throw new Exception($"Error converting argument \"{name}\" to {map[name].Name}", e);
					}
					name = null;
				}
			foreach (var v in this)
				if (v.Value == null)
					throw new Exception($"Argument \"{v.Key}\" is required");
			return this;
		}
		public T To<T>(string name) => (T)Convert.ChangeType(this[name], typeof(T));
	}
}