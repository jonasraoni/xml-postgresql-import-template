# High performance XML > PostgreSQL import template in C# 

I've written this to import a huge amount of data from XMLs into a PostgreSQL database and as I had the opportunity to try out several techniques, I've decided to keep it roughly documented here to reuse the ideas =)

# Design
The application is designed in a "producer <=> storage <=> consumer" fashion. The producer starts out by taking an empty bag from the storage and then, asynchronously and concurrently, extracts records from the XML into it. Once the bag is full, it is sent to the storage, the first consumer on the waiting line is notified, takes the bag, inserts the records into the database and send the bag back to the storage for recycling.

There may be N producers/consumers, they run in parallel and share a single storage instance.

## Storage
A generic concurrent queue, featuring recycling of buffers and a waiting line, which is done by blocking the calling thread. It's used to keep batches of records (represented by `Member` instances inherited from a Dictionary).

In order to reduce garbage collection calls I've tried to transform the `Member` class into a `struct` (which turns the class into a fixed size value type in C#), plain object and also to reuse the instances, but the improvements didn't pay off in my environment or brought too much complexity.

## Consumer
Moves data from the storage, while it's opened, into the database, once it's over it pauses until there's a new batch available. In case it fails, the batch is returned to the storage, so another consumer can handle it. The consumers also work asynchronously and concurrently.

### Database Techniques
- Batches: Data is inserted into the database by batches, on my system a value of 10K items was a good deal. At first I tried to fill a temporary table, large enough to be kept in memory, with a prepared insert statement made of several `values` (e.g. VALUES(...), VALUES(...), ...), it was fast, but not faster than using the binary `COPY` format.
- Storage: A non-indexed temporary table is used to save the batch (`temp_buffers` is set to batch-size/500 MB to accommodate the sample data in memory safely).
- Transactions: I was in doubt between covering the code with a big win/lose transaction or issuing them by batches, I didn't see much improvement on the first, so every batch is covered with a transaction.
- Indexing: The sample table has no primary key nor index, so there's an argument to create a unique key, which is a big performance winner.
- Upsert: With an indexed table I'm able to use the new "upsert" syntax. Without it, I could use an UPDATE followed by an INSERT, which has a longer syntax and raises the problem of comparing NULLS (I've tried to use the new `fieldA IS NOT DISTINCT FROM fieldB` syntax, which checks for nulls, but it's ultimately slow, it looks like it doesn't use indexes).
- Validation: Members are pre-validated locally to avoid spending database resources with invalid items.
- "Hash" column: I was going to add a `hash` column to update the record only if the hash of its data was changed, but for the sample table it isn't worth as there's just one field to update.

## Producer
Extracts batches of data from the XML into the storage and pauses once the storage is full.

To work in parallel each producer has a starting point in the XML and they avoid collisions by skipping records, I just implemented it this way out of curiosity... But IF parsing XML was the bottleneck, maybe a RAID structure with several workers plus reverse parsers (starting to read from the end of the XML) could improve the performance.

### XML Parser
I wrote a generic XML finder using the standard SAX library of C#, which already has some integrity checks bundled.
I thought about writing it using iterators `foreach(person in find("/person"))`, but as it was slow I decided to go on with another approach... What came in my mind was a functional `person.find("person", () => find("first-name"))` and a standard `person = find("person")` => `person.find("first-name")` way. I've implemented both ways and according to the profiler their performance was the same for the use case... In the end I decided to stay with what I consider the simplest: `context = find("person")` => `find("first-name", context)`, a kind of "string.indexOf".

The `find` method uses a query based on XPath (I've just added two types, queries based on the root `/node` and anywhere `person/first-name`) and drills down until the end of the file when no `context` is available. When a match is found, the current depth is returned, which can be used as a context to query children nodes.

# Running
There's a sample XML file and a `CREATE TABLE` statement in the [/data](data) folder.

The application smoothly compiles in Windows and Linux (Mono [https://www.mono-project.com/] required).
As there's an official Docker image with Mono, follow below a snippet to build and run it. It just requires sharing the application folder and setting the required arguments:

```bash
docker run -it --rm -v **APPLICATION FOLDER**:/home mono:latest bash -c "cd /home \
&& nuget restore \
&& msbuild /P:Configuration=Release \
&& mono bin/Release/Synchronizer.exe user **USER** pass **PASSWORD** host **HOST** db **DATABASE** path /home/data/UPDATE.xml"
```

## Parameters
The parameters for the executable are in the format `name value[ name value...]`:
- host (localhost): Database host
- port (5432): Database port
- user: Database username
- pass: Database password
- db: Database name
- path: Path of the "update-file.xml"
- batch (10000): The amount of records in the batch
- buffer (4): Amount of batches that will be kept in the storage
- index (true): Whether to create an unique index
- upsert (true): Whether to use the upsert syntax
- maxlength (255): Max length of data that the Parser will attempt to read from a text node
timeout (120): Database communication timeout in seconds
- p-workers (2): Amount of producer workers
- c-workers (4): Amount of consumer workers

# Considerations
- Wherever possible, I used asynchronous operations/thread blocking to allow the CPUs to process other things.
- The SQL matching is solely handled by the operator "=", there's no collation/case sensitivity enforcement.
- There's a small defensive check to avoid memory exhaustion when reading data (the default for this sample is 255 characters and can be set to unlimited).
- The default values for the arguments are based on my system (6-core processor + old SSD disk).