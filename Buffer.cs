namespace Synchronizer {
	/// <summary>
	/// A composite type made of a simple array with a counter, it's used to store the batches in memory until it's time to process them.
	/// </summary>
	public class Buffer {
		public Buffer(int length) {
			data = new Member[length];
			Count = 0;
		}
		public int Count { get; private set; }
		public bool IsFull => Count >= data.Length;
		public Member this[int index] => data[index];
		public void Clear() => Count = 0;
		public void Add(Member person) => data[Count++] = person;
		private Member[] data;
	}
}