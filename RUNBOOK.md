# 5. Runbook

## 5.1 Prerequisites

You need:

* Python 3.11+  
* PostgreSQL running locally or reachable  
* RustFS running and S3-compatible endpoint reachable  
* OpenSSL for self-signed TLS certs

Example versions used in this design:

* PostgreSQL 15+  
* Python 3.11  
* PyArrow 17+  
* RustFS with S3 API on http://localhost:9000

---

## 5.2 Create PostgreSQL database

Option A: using ```createdb```  
```bash  
createdb flight_demo  
```

Option B: using ```psql```  

```bash  
psql -U postgres -h localhost -p 5432 -f scripts/create_db.sql  
```

Then ensure your ```.env``` has the correct credentials.  

Example:  

```dotenv  
PGHOST=localhost  
PGPORT=5432  
PGDATABASE=flight_demo  
PGUSER=postgres  
PGPASSWORD=postgres  
PGSCHEMA=flight_demo  
```

## 5.3 Create Python virtual environment

```bash  
cd arrow-flight-postgres-demo

python -m venv .venv  
source .venv/bin/activate

pip install -r requirements.txt  
```

On Windows PowerShell:  

```powershell  
python -m venv .venv  
.venv\Scripts\Activate.ps1  
pip install -r requirements.txt  
```

## 5.4 Configure RustFS

Create the basket in rustfs:  


Ensure RustFS is running. Example expected settings:  

```dotenv  
RUSTFS_ENDPOINT=http://localhost:9000  
RUSTFS_ACCESS_KEY=rustfsadmin  
RUSTFS_SECRET_KEY=rustfsadmin  
RUSTFS_BUCKET=arrow-flight-demo  
RUSTFS_REGION=us-east-1  
```

If your RustFS credentials differ, update ```.env```.  

You can test RustFS connectivity later with:  
```bash  
python -m src.storage.rustfs --list --prefix ""  
```

## 5.5 Generate synthetic data


Start with a small dataset to validate everything:  

```bash  
uv run python -m src.etl.generate_synthetic --events-rows 200000 --sales-rows 100000 --batch-size 50000 --upload
  
```

This creates:  

```text  
data/csv/events.csv  
data/csv/sales.csv  
data/parquet/events/part-0000.parquet  
data/parquet/sales/part-0000.parquet  
data/manifest.json  
```

And, if RustFS is configured and ```--upload``` is passed, uploads:  

```text  
s3://arrow-flight-demo/demo/events/part-0000.parquet  
s3://arrow-flight-demo/demo/sales/part-0000.parquet  
```

Verify RustFS objects:  
```bash  
uv run python -m src.storage.rustfs --list --prefix demo/  
```

For a larger, article-like workload:  
```bash  
uv run python -m src.etl.generate_synthetic --events-rows 10000000 --sales-rows 5000000 --batch-size 100000 --upload
```
Warning: 10M rows can take significant time, disk, and memory. Start small first.  


## 5.6 Load synthetic data into PostgreSQL

```bash  
uv run python -m src.etl.load_postgres --manifest data/manifest.json  
```

This:

* creates schema ```flight_demo```  
* creates tables:  
  * ```flight_demo.events```  
  * ```flight_demo.sales```  
  * ```flight_demo.flight_upload_demo```  
* truncates them  
* loads CSV data using PostgreSQL ```COPY```  
* runs ```ANALYZE```


Validate:  

```bash  
psql -U postgres -h localhost -d flight_demo -c "SET search_path TO flight_demo; SELECT count(*) FROM events;"  
psql -U postgres -h localhost -d flight_demo -c "SET search_path TO flight_demo; SELECT count(*) FROM sales;"  
```

# 6. Run Arrow Flight server

## 6.1 Minimal plaintext server

Terminal 1:  
```bash  
uv run python -m src.flight.server_minimal --port 8815 --no-auth  
```
This starts a Flight server on:  
```text  
grpc://0.0.0.0:8815  
```

Clients should use:  

```text  
grpc://localhost:8815  
```

## 6.2 Test a simple Flight read

Terminal 2:  
```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"  
```
Expected output includes:

* row count  
* Arrow schema  
* first few rows as Python dict


## 6.3 Read a named dataset by path

```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --path events  
```
This uses:  
```python  
FlightDescriptor.for_path("events")  
```

## 6.4 List available flights

```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --list-flights  
```

# 7. Benchmark JDBC/ODBC-like row transport vs Arrow Flight

The article’s core point is:  
```Query time may be small; transport time may dominate.```  

This project uses PostgreSQL’s Python DB-API row cursor as a practical stand-in for JDBC/ODBC row-oriented transport.  

Start the minimal Flight server if it is not already running:  
```bash  
uv run python -m src.flight.server_minimal --port 8815 --no-auth  
```

Then run:  
```bash  
uv run python -m src.transport.benchmark --flight-uri grpc://localhost:8815 --query "SELECT * FROM events WHERE day='2026-08-01'" --fetch-sizes 1000 10000 50000  
```
What you should see:

* PostgreSQL engine time from ```EXPLAIN ANALYZE```  
* row-oriented fetch wall-clock times  
* Arrow Flight wall-clock time  
* rows/sec comparison

Interpretation:

* The row path pays per-cell fetch/materialization overhead.  
* The Flight path receives Arrow IPC columnar batches.  
* On larger result sets, the Flight advantage grows.

If you want to avoid pandas materialization in the row path:  
```bash  
uv run python -m src.transport.benchmark --flight-uri grpc://localhost:8815 --query "SELECT * FROM events WHERE day='2026-08-01'" --no-materialize  
```

# 8. Parallel endpoints

Arrow Flight’s parallelism comes from returning multiple endpoints in ```FlightInfo```.  

Start a sharded server:  
```bash  
uv run python -m src.flight.server_sharded --port 8815 --shards 4 --no-auth  
```

Now a single ```GetFlightInfo``` returns 4 endpoints.  

Run a parallel client read:  

```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --parallel --sql "SELECT * FROM events WHERE day='2026-08-01'"  
```
The client:

1. calls ```get_flight_info```  
2. receives 4 endpoints  
3. spawns 4 concurrent ```do_get``` streams  
4. concatenates the resulting Arrow tables

You can also benchmark parallel Flight:  
```bash  
uv run python -m src.transport.benchmark --flight-uri grpc://localhost:8815 --parallel-flight --query "SELECT * FROM events WHERE day='2026-08-01'"  
```

# 9. Distributed coordinator / worker pattern

This demonstrates the article’s key distributed-read idea:

* coordinator plans the query  
* coordinator returns endpoints pointing to workers  
* workers stream data directly to client  
* coordinator moves metadata only, not bulk data

## 9.1 Start worker 1

Terminal 1:  
```bash  
uv run python -m src.flight.server_sharded --port 8816 --shards 1 --no-auth
```
## 9.2 Start worker 2

Terminal 2:  
```bash  
uv run python -m src.flight.server_sharded --port 8817 --shards 1 --no-auth
```

## 9.3 Start coordinator

Terminal 3:  
```bash  
uv run python -m src.flight.server_sharded --port 8815 --shards 4 --workers grpc://localhost:8816 grpc://localhost:8817 --no-auth  
```
The coordinator returns 4 endpoints:

* endpoint 0 -> worker 8816  
* endpoint 1 -> worker 8817  
* endpoint 2 -> worker 8816  
* endpoint 3 -> worker 8817

## 9.4 Client fans out to workers

Terminal 4:  
```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --parallel --sql "SELECT * FROM events WHERE day='2026-08-01'"  
```

The client contacts the coordinator once for metadata, then reads directly from workers.  
---

# 10. TLS and bearer-token authentication

## 10.1 Generate self-signed certificates

From project root:  
```bash  
mkdir -p certs

openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes -keyout certs/server.key -out certs/server.crt -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

cp certs/server.crt certs/ca.pem  
chmod 600 certs/server.key  
```

Set a token in ```.env``` or pass it on the command line:  
```bash  
export FLIGHT_AUTH_TOKEN=demo-secret-token  
```

## 10.2 Start secure Flight server

```bash  
uv run python -m src.flight.server_minimal --port 8815 --tls --token "$FLIGHT_AUTH_TOKEN"  
```
Server listens on:  
```text  
grpc+tls://0.0.0.0:8815  
```

## 10.3 Secure client read

```bash  
uv run python -m src.flight.client --uri grpc+tls://localhost:8815 --ca certs/ca.pem --token "$FLIGHT_AUTH_TOKEN" --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"  
```
What this proves:

* auth is enforced in middleware, not inside each method  
* TLS encrypts Arrow buffers on the wire  
* client injects bearer token using client middleware



## 10.4 Negative auth test

Bad token should fail:  
```bash  
uv run python -m src.flight.client --uri grpc+tls://localhost:8815 --ca certs/ca.pem --token wrong-token --sql "SELECT 1"  
```

Expected:  
```text  
FlightUnauthenticatedError  
```

# 11. DoPut ingest and DoAction commit

This demonstrates the write path:

* client uploads Arrow record batches using ```DoPut```  
* server stages them in memory  
* client triggers ```DoAction("commit_ingest")```  
* server inserts staged batches into PostgreSQL


Make sure a plaintext server is running:  

```bash  
uv run python -m src.flight.server_minimal --port 8815 --no-auth  
```

Then upload a slice of generated Parquet:  
```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --put-parquet data/parquet/events/part-0000.parquet --put-path upload_demo --put-rows 10000 --commit-table flight_upload_demo  
```
Check PostgreSQL:  
```bash  
psql -U postgres -h localhost -d flight_demo -c "SET search_path TO flight_demo; SELECT count(*) FROM flight_upload_demo;"  
```

List staged datasets:
without committing:
```bash  

uv run python -m src.flight.client --uri grpc://localhost:8815 --put-parquet data/parquet/events/part-0000.parquet --put-path upload_demo --put-rows 10000
```

List staged datasets:
```bash
uv run python -m src.flight.client --uri grpc://localhost:8815 --list-staging
```

Commit staged dataset:
```bash
uv run python -m src.flight.client --uri grpc://localhost:8815 --put-parquet data/parquet/events/part-0000.parquet --put-path upload_demo --put-rows 10000 --commit-table flight_upload_demo

```


# 12. Prepared statement demo over Flight

This is not official Flight SQL Protobuf, but it demonstrates the same protocol shape:

* create prepared statement using DoAction  
* bind parameters using DoPut  
* execute using GetFlightInfo \+ DoGet

Start server:  
```bash  
uv run python -m src.flight.server_minimal --port 8815 --no-auth  
```

Run:  
```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --prepared-demo  
```

The demo executes:  
```sql  
SELECT id, kind, amount  
FROM events  
WHERE day=%s AND kind=%s  
```

with bound parameters:  


```python  
(date(2026, 8, 1), "purchase")  
```

```sql
SELECT id, kind, amount, day  
FROM flight_demo.events  
WHERE day='2026-08-01' AND kind='purchase';

# 13. Catalog metadata demo

The Flight-SQL-style server supports metadata commands:

* ```get_catalogs```  
* ```get_db_schemas```  
* ```get_tables```

Start server:  

```bash  
uv run python -m src.flight.server_minimal --port 8815 --no-auth  
```

Run:  
```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --metadata get_tables  
```

Also:

```bash  
uv run python -m src.flight.client --uri grpc://localhost:8815 --metadata get_db_schemas  
```

# 14. Official ADBC Flight SQL client path

The runnable server in this project uses JSON command envelopes over raw Flight to demonstrate Flight SQL concepts without implementing the full official Protobuf protocol.  
For real Flight SQL, use the ADBC client module against a compliant Flight SQL server.  
Examples of real Flight SQL servers:

* Dremio Flight SQL endpoint  
* InfluxDB 3.x / IOx Flight SQL endpoint  
* Ballista  
* an ADBC Flight SQL server wrapping another ADBC driver

Set environment variables:  
```bash  
export FLIGHT_SQL_URI=grpc+tls://your-flightsql-server:32010  
export FLIGHT_SQL_TOKEN="Bearer your-token"  
export FLIGHT_SQL_CA=/path/to/ca.pem  
```
Run:  
```bash  
python -m src.flight_sql.adbc_client \  
  --uri "$FLIGHT_SQL_URI" \  
  --sql "SELECT region, sum(amount) AS total FROM sales GROUP BY region" \  
  --token "$FLIGHT_SQL_TOKEN" \  
  --ca "$FLIGHT_SQL_CA"  
```

Parameterized query example:  
```bash  
python -m src.flight_sql.adbc_client \  
  --uri "$FLIGHT_SQL_URI" \  
  --sql "SELECT id, kind, ts FROM events WHERE day=? AND kind=?" \  
  --params '["2026-08-01", "purchase"]' \  
  --token "$FLIGHT_SQL_TOKEN" \  
  --ca "$FLIGHT_SQL_CA"  
```
This is the production-style JDBC/ODBC replacement path:

* Python analytics: ADBC Flight SQL driver  
* BI tools: ODBC-over-Flight-SQL shim  
* Java services: JDBC-over-Flight-SQL driver

---

# 15. Concept coverage map

| Article concept | Where it is covered in this project |
| ----- | ----- |
| JDBC/ODBC row-major serialization tax | src/transport/benchmark.py compares PostgreSQL row cursor fetch vs Flight |
| Engine time vs transport time decomposition | explain_analyze_time_ms() and benchmark output |
| Arrow IPC columnar wire | Flight server streams RecordBatch objects via GeneratorStream |
| Zero-deserialization receive | Client uses do_get(...).read_all() to produce pyarrow.Table |
| gRPC control plane / Arrow data plane | get_flight_info() returns metadata; do_get() streams data |
| FlightDescriptor | for_command(sql) and for_path(dataset) used in server/client |
| FlightInfo | Returned by get_flight_info() |
| Ticket | Opaque JSON ticket used by do_get() |
| Endpoint | One or more endpoints returned by server |
| Location | Worker/coordinator URIs in endpoints |
| DoGet | Read path in server_minimal.py and server_sharded.py |
| DoPut | Ingest path and prepared parameter binding |
| DoAction | Prepared statements, transactions, commit ingest, list staging |
| ListFlights | list_flights() in server and client |
| Parallel endpoints | server_sharded.py + read_parallel_uri() |
| Distributed reads without coordinator funnel | coordinator/worker runbook in section 9 |
| Flight SQL standardization idea | JSON command envelopes for query/catalog/prepared/transaction |
| ADBC Flight SQL client | src/flight_sql/adbc_client.py |
| Prepared statements | prepared_demo() in client and server actions |
| Parameter binding | DoPut with Arrow parameter batch |
| Catalog metadata | get_catalogs, get_db_schemas, get_tables |
| Auth middleware | src/flight/auth.py |
| TLS | --tls, grpc+tls://, tls_certificates, tls_root_certs |
| Streaming large results | GeneratorStream + PostgreSQL server-side cursor |
| RustFS object storage | Generated Parquet uploaded via src/storage/rustfs.py |
| Synthetic data generation | src/etl/generate_synthetic.py |
| PostgreSQL loading | src/etl/load_postgres.py |

---

# 16. Recommended test sequence

From project root, with PostgreSQL and RustFS running:  

```bash  
# 1. Activate env  
source .venv/bin/activate

# 2. Create DB if needed  
createdb flight_demo

# 3. Generate small data  
python -m src.etl.generate_synthetic \  
  --events-rows 200000 \  
  --sales-rows 100000 \  
  --batch-size 50000 \  
  --upload

# 4. Load PostgreSQL  
python -m src.etl.load_postgres --manifest data/manifest.json

# 5. Start minimal Flight server in another terminal  
python -m src.flight.server_minimal --port 8815 --no-auth

# 6. Basic Flight read  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"

# 7. Benchmark row vs Flight  
python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --query "SELECT * FROM events WHERE day='2026-08-01'" \  
  --fetch-sizes 1000 10000 50000

# 8. Parallel endpoints  
python -m src.flight.server_sharded --port 8815 --shards 4 --no-auth

python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --parallel \  
  --sql "SELECT * FROM events WHERE day='2026-08-01'"

# 9. TLS \+ auth  
openssl req -x509 -newkey rsa:4096 -sha256 -days 365 -nodes \  
  -keyout certs/server.key \  
  -out certs/server.crt \  
  -subj "/CN=localhost" \  
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

cp certs/server.crt certs/ca.pem

export FLIGHT_AUTH_TOKEN=demo-secret-token

python -m src.flight.server_minimal \  
  --port 8815 \  
  --tls \  
  --token "$FLIGHT_AUTH_TOKEN"

python -m src.flight.client \  
  --uri grpc+tls://localhost:8815 \  
  --ca certs/ca.pem \  
  --token "$FLIGHT_AUTH_TOKEN" \  
  --sql "SELECT * FROM events WHERE day='2026-08-01' LIMIT 100"

# 10. DoPut ingest  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --put-parquet data/parquet/events/part-0000.parquet \  
  --put-path upload_demo \  
  --put-rows 10000 \  
  --commit-table flight_upload_demo

# 11. Prepared demo  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --prepared-demo

# 12. Metadata demo  
python -m src.flight.client \  
  --uri grpc://localhost:8815 \  
  --metadata get_tables  
```

# 17. Troubleshooting

## PostgreSQL connection fails

Check:  
```bash  
psql -U postgres -h localhost -p 5432 -d flight_demo -c "SELECT 1;"
```  

Verify ```.env```:  

```dotenv  
PGHOST=localhost  
PGPORT=5432  
PGDATABASE=flight_demo  
PGUSER=postgres  
PGPASSWORD=postgres  
```

## RustFS upload fails

Check endpoint:  
```bash  
curl http://localhost:9000/minio/health/live  
```
Or simply:  
```bash  
curl -v http://localhost:9000  
```

Verify credentials in ```.env```.  

Test listing:  

```bash  
python -m src.storage.rustfs --list --prefix ""  
```

## Flight client cannot connect

Ensure server is running and URI matches TLS setting:

* plaintext: ```grpc://localhost:8815```  
* TLS: ```grpc+tls://localhost:8815```


If TLS server uses self-signed cert, pass CA:  
```bash  
--ca certs/ca.pem  
```

## Auth fails

Ensure client token exactly matches server token:  

```bash  
--token "$FLIGHT_AUTH_TOKEN"  
```
Header must be:  
```text  
Authorization: Bearer <token\>  
```

The client middleware injects this automatically.  


## Parallel read fails with “column id does not exist”

The sharded server partitions using:  
```sql  
mod(id, N) = partition_index  
```

So the base query must expose an id column.  
The bundled datasets do:  
```sql  
SELECT id, ... FROM events  
SELECT id, ... FROM sales  
```

If you supply a custom query, include id.  


## ADBC Flight SQL client fails against local Flight server

That is expected.  
The local server is raw Flight / Flight-SQL-style JSON, not official Flight SQL Protobuf.  
Use ```src.flight_sql.adbc_client``` only against a real Flight SQL server.  


---

# 18. What to scale up for article-like benchmarks

For the article’s “10M rows” style example:  

```bash  
python -m src.etl.generate_synthetic \  
  --events-rows 10000000 \  
  --sales-rows 5000000 \  
  --batch-size 100000 \  
  --upload

python -m src.etl.load_postgres  
```

Then benchmark:  
```bash  
python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --query "SELECT * FROM events WHERE day='2026-08-01'" \  
  --fetch-sizes 1000 10000 50000 100000  
```

For parallel Flight:  
```bash  
python -m src.flight.server_sharded --port 8815 --shards 8 --no-auth

python -m src.transport.benchmark \  
  --flight-uri grpc://localhost:8815 \  
  --parallel-flight \  
  --query "SELECT * FROM events WHERE day='2026-08-01'"  
```

You should observe:

* PostgreSQL engine time remains relatively stable.  
* Row-oriented transport dominates wall-clock.  
* Flight transport collapses the overhead.  
* Parallel endpoints improve ability to saturate network/client cores.

---

# 19. Production notes

This project is designed as a learning/interview/demo system. For production:

1. Replace JSON command envelopes with official Flight SQL Protobuf if you need third-party Flight SQL drivers.  
2. Use a real Flight SQL server or implement the official protocol.  
3. Put PostgreSQL behind a proper query engine if you need true analytical columnar execution.  
4. Use managed TLS certificates, not self-signed.  
5. Validate JWTs, scopes, and row/column policies in middleware.  
6. Add observability:  
   * request tracing  
   * metrics for rows/sec, bytes/sec, endpoint count  
   * slow-query logging  
7. Tune Arrow batch size to balance memory and latency.  
8. Ensure network infrastructure supports long-lived HTTP/2 gRPC streams:  
   * no aggressive L7 idle timeouts  
   * HTTP/2 end-to-end  
   * gRPC-aware load balancing if needed  
9. For very large PostgreSQL result sets, consider:  
   * server-side cursors  
   * partition pruning  
   * materialized views  
   * columnar acceleration layer  
   * direct Parquet/RustFS reads for analytical consumers

---

# 20. Minimal “proof it works” checklist

Run these in order:  
```bash  
python -m src.etl.generate_synthetic --events-rows 50000 --sales-rows 20000 --batch-size 10000 --upload  

python -m src.etl.load_postgres  

python -m src.flight.server_minimal --port 8815 --no-auth  

python -m src.flight.client --uri grpc://localhost:8815 --sql "SELECT * FROM events LIMIT 10"  

python -m src.transport.benchmark --flight-uri grpc://localhost:8815 --query "SELECT * FROM events WHERE day='2026-08-01'" --fetch-sizes 1000 10000  

python -m src.flight.server_sharded --port 8815 --shards 4 --no-auth  

python -m src.flight.client --uri grpc://localhost:8815 --parallel --sql "SELECT * FROM events WHERE day='2026-08-01'"  

python -m src.flight.client --uri grpc://localhost:8815 --prepared-demo  

python -m src.flight.client --uri grpc://localhost:8815 --metadata get_tables  

python -m src.storage.rustfs --list --prefix demo/  
```

If all of those succeed, the project is covering the article’s main mechanics on your local PostgreSQL + RustFS setup.
