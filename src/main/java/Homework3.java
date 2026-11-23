import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Homework 3 - "Homework 2 revisited"
 *
 * Based on Blocking / RestartBlocking / NoLiveLocks from Homework 2
 * (original author: Martin Schaeler, MIT license - see Homework 2 files).
 *
 * Usage (stdout = transaction output (sums), stderr = statistics):
 *
 *   1b/1d  java Homework3 demo  <seconds> <threads>          continuous stream + live stats every second
 *   1c     java Homework3 sweep <seconds> [optimized]        throughput for 1,2,4,8,16,32 threads
 *   2a     java Homework3 serial <numTx>                     trivial serial execution (1 thread, 1 connection)
 *   2a     java Homework3 concurrent <numTx> <threads>       same stream, baseline concurrent execution
 *   2c     java Homework3 optimized <numTx> <threads>        same stream, optimized execution
 *   2d     java Homework3 compare <numTx> <threads>          serial vs. concurrent vs. optimized (one table)
 *   2b/2c  java Homework3 ablation <numTx> <threads>         switch on the fixes one by one (effect of each fix)
 *
 * Tip: redirect the transaction output, e.g.  java Homework3 compare 20000 8 > tx_output.log
 */
public class Homework3 {

    public static int num_queries_per_task = 3;
    static final int    NUM_TUPLE = 50000;
    static final double MAX_VALUE = 50.0;
    static final long   SEED      = 123456L;

    // ------------------------------------------------------------------
    // 1a) Transaction types and their probabilities
    //     p <  0.6        -> multi-point read  (60 %)
    //     0.6 <= p < 0.7  -> insert            (10 %)
    //     0.7 <= p < 0.8  -> full table scan   (10 %)
    //     p >= 0.8        -> write (3 updates) (20 %)
    // ------------------------------------------------------------------
    enum TxType { MULTI_POINT_READ, INSERT, FULL_SCAN, WRITE }

    /** Description of ONE transaction: what it does, generated before it runs. */
    static final class TransectionSpec {
        final long   index;   // position in the transaction stream
        final TxType type;    //enums
        final int[]    ids;   // tuples to read / update
        final double[] values;// new values for updates / insert

        TransectionSpec(long index, TxType type, int[] ids, double[] values) {
            this.index = index;
            this.type = type;
            this.ids = ids;
            this.values = values;
        }
    }
    /**
     * 1a) Dice p in [0,1] i.i.d. for every transaction.
     * The random generator is seeded with (SEED, index), so transaction number i is ALWAYS the
     * same transaction - no matter which thread executes it or in which mode (serial / concurrent).
     * This gives us "a given stream of transactions" that can be replayed for a fair comparison (2a, 2d).
     */
    static TransectionSpec generate(long index) {
        SplittableRandom r = new SplittableRandom(SEED ^ (index * 0x9E3779B97F4A7C15L));
        double p = r.nextDouble();
        if (p < 0.6) {                                   // 1a-b) read-only multi-point query
            return new TransectionSpec(index, TxType.MULTI_POINT_READ, distinctIds(r, num_queries_per_task), null);
        } else if (p < 0.7) {                            // 1a-c) insert one new tuple
            return new TransectionSpec(index, TxType.INSERT, null, new double[]{ r.nextDouble() * MAX_VALUE });
        } else if (p < 0.8) {                            // 1a-d) full table scan
            return new TransectionSpec(index, TxType.FULL_SCAN, null, null);
        } else {                                         // 1a-e) write: 3 single-tuple updates
            int[] ids = distinctIds(r, num_queries_per_task);
            double[] vals = new double[num_queries_per_task];
            for (int q = 0; q < vals.length; q++) vals[q] = r.nextDouble() * MAX_VALUE;
            return new TransectionSpec(index, TxType.WRITE, ids, vals);
        }
    }

    /** k distinct random tuple ids out of the original NUM_TUPLE tuples. */
    static int[] distinctIds(SplittableRandom r, int k) {
        int[] ids = new int[k];
        for (int i = 0; i < k; i++) {
            int candidate;
            boolean dup;
            do {
                candidate = r.nextInt(NUM_TUPLE);
                dup = false;
                for (int j = 0; j < i; j++) if (ids[j] == candidate) { dup = true; break; }
            } while (dup);
            ids[i] = candidate;
        }
        return ids;
    }

    // ------------------------------------------------------------------
    // Options: which optimizations (2c) are switched on.
    // ------------------------------------------------------------------
    static final class Options {
        String  name;
        boolean cachedParamStatements; // prepare statements ONCE per connection, task id as ? parameter
        boolean quietBufferedOutput;   // no "Done task ..." line, sums written to a buffered stream
        boolean autocommitReads;       // read-only queries without explicit BEGIN/COMMIT round trip
        boolean asyncCommit;           // SET synchronous_commit = off (no WAL fsync wait at COMMIT)
        boolean hotFillfactor;         // table created WITH (fillfactor = 80) -> HOT updates, less bloat
        boolean fastRetry;             // tiny random back-off instead of exponential back-off
        boolean coveringIndex;         // extra index on data_value -> sum() as index-only scan (experiment)

        static Options baseline() { Options o = new Options(); o.name = "baseline"; return o; }

        static Options optimized() {
            Options o = new Options(); o.name = "optimized";
            o.cachedParamStatements = o.quietBufferedOutput = o.autocommitReads =
                    o.asyncCommit = o.hotFillfactor = o.fastRetry = true;
            return o;
        }

        Options copy(String newName) {
            Options o = new Options(); o.name = newName;
            o.cachedParamStatements = cachedParamStatements; o.quietBufferedOutput = quietBufferedOutput;
            o.autocommitReads = autocommitReads; o.asyncCommit = asyncCommit;
            o.hotFillfactor = hotFillfactor; o.fastRetry = fastRetry; o.coveringIndex = coveringIndex;
            return o;
        }
    }

    // ------------------------------------------------------------------
    // Statistics of one run
    // ------------------------------------------------------------------
    static final class Stats {
        final AtomicLong committed = new AtomicLong();
        final AtomicLong retries   = new AtomicLong();      // deadlock / serialization failures
        final AtomicLong[] perType = new AtomicLong[TxType.values().length];
        final AtomicInteger inFlight = new AtomicInteger(); // transactions currently running
        Stats() { for (int i = 0; i < perType.length; i++) perType[i] = new AtomicLong(); }
    }

    /** Next id for inserted tuples (ids >= NUM_TUPLE are inserted ones). Atomic -> no duplicate keys. */
    static final AtomicInteger nextInsertId = new AtomicInteger(NUM_TUPLE);

    // ------------------------------------------------------------------
    // Database setup (NOT part of the measured time)
    // Re-creating the table before every run also removes the tuples inserted by
    // ------------------------------------------------------------------
    static void createDB(Options o) {
        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            con.setAutoCommit(true);
            try (Statement st = con.createStatement()) {
                st.execute("DROP TABLE IF EXISTS blocking_data");
                // Same schema as in Homework 2. Only a storage parameter is added in the optimized version.
                st.execute("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer)"
                        + (o.hotFillfactor ? " WITH (fillfactor = 80)" : ""));
            }
            con.setAutoCommit(false);
            SplittableRandom r = new SplittableRandom(SEED);
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)")) {
                for (int i = 0; i < NUM_TUPLE; i++) {
                    ps.setInt(1, i);
                    ps.setDouble(2, r.nextDouble() * MAX_VALUE);
                    ps.setInt(3, 0);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            con.commit();
            con.setAutoCommit(true);
            try (Statement st = con.createStatement()) {
                if (o.coveringIndex) st.execute("CREATE INDEX blocking_data_value_idx ON blocking_data (data_value)");
                st.execute("VACUUM ANALYZE blocking_data");   // fresh statistics + visibility map
                st.execute("CHECKPOINT");                     // every run starts with the same clean state
            } catch (SQLException e) {
                System.err.println("(info) " + e.getMessage());
            }
            nextInsertId.set(NUM_TUPLE);
        } catch (SQLException e) {
            throw new RuntimeException("Could not create database", e);
        }
    }

    // ------------------------------------------------------------------
    // Worker = one thread with ONE connection that executes transactions one after another.
    // ------------------------------------------------------------------
    static final class Worker {
        final Connection con;
        final Options o;
        final Stats stats;
        final PrintStream out;
        // cached statements (only used when o.cachedParamStatements)
        PreparedStatement psPoint, psScan, psInsert, psUpdate;

        Worker(Options o, Stats stats, PrintStream out) throws SQLException {
            this.o = o;
            this.stats = stats;
            this.out = out;
            this.con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            if (con == null) throw new SQLException("no connection");
            con.setAutoCommit(false);
            con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            if (o.asyncCommit) {
                try (Statement st = con.createStatement()) {
                    st.execute("SET synchronous_commit = off");
                }
                con.commit();
            }
            if (o.cachedParamStatements) {
                psPoint  = con.prepareStatement("SELECT data_value FROM blocking_data WHERE data_id = ?");
                psScan   = con.prepareStatement("SELECT sum(data_value) FROM blocking_data");
                psInsert = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)");
                psUpdate = con.prepareStatement("UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?");
            }
        }

        void close() { try { con.close(); } catch (SQLException ignored) { } }

        /** Executes one transaction until it commits (retry on deadlock) - every transaction eventually commits. */
        void execute(TransectionSpec t) {
            stats.inFlight.incrementAndGet();
            int attempt = 0;
            while (true) {
                try {
                    runOnce(t);
                    break;                                             // committed
                } catch (SQLException e) {
                    attempt++;
                    stats.retries.incrementAndGet();
                    try { con.rollback(); } catch (SQLException ignored) { }
                    String state = e.getSQLState();
                    // 40P01 = deadlock_detected, 40001 = serialization_failure, 55P03 = lock_not_available
                    if (!"40P01".equals(state) && !"40001".equals(state) && !"55P03".equals(state)) {
                        throw new RuntimeException("Unexpected error in tx " + t.index + " (" + state + ")", e);
                    }
                    backoff(attempt);
                }
            }
            stats.inFlight.decrementAndGet();
            stats.committed.incrementAndGet();
            stats.perType[t.type.ordinal()].incrementAndGet();
        }

        void backoff(int attempt) {
            try {
                if (o.fastRetry) {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1, 6));    // 1..5 ms jitter
                } else {                                                       // NoLiveLocks (Homework 2)
                    Thread.sleep((long) (10 * Math.pow(2, Math.min(attempt, 5))) + ThreadLocalRandom.current().nextInt(50));
                }
            } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
        }

        void runOnce(TransectionSpec t) throws SQLException {
            switch (t.type) {
                case MULTI_POINT_READ: multiPointRead(t); break;
                case INSERT:           insert(t);         break;
                case FULL_SCAN:        fullScan(t);       break;
                case WRITE:            write(t);          break;
            }
        }

        // Homework-2 style: a new statement per transaction, with scroll-sensitive / updatable flags
        PreparedStatement prepareBaseline(String sql) throws SQLException {
            return con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        }

        /** 1a-b) read data_value of 3 distinct tuples, one statement per tuple, print the sum. */
        void multiPointRead(TransectionSpec t) throws SQLException {
            if (o.autocommitReads) con.setAutoCommit(true);
            try {
                PreparedStatement ps = o.cachedParamStatements ? psPoint
                        : prepareBaseline("SELECT data_value FROM blocking_data WHERE data_id = ?");
                double sum = 0;
                for (int id : t.ids) {
                    ps.setInt(1, id);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) sum += rs.getDouble(1);
                    }
                }
                if (!o.cachedParamStatements) ps.close();
                if (!o.autocommitReads) con.commit();
                out.println("TransectionSpec " + t.index + " MULTI_POINT_READ sum=" + sum);
                logDone(t);
            } finally {
                if (o.autocommitReads) con.setAutoCommit(false);
            }
        }

        /** 1a-d) SELECT sum(data_value) FROM blocking_data, print the result. */
        void fullScan(TransectionSpec t) throws SQLException {
            if (o.autocommitReads) con.setAutoCommit(true);
            try {
                PreparedStatement ps = o.cachedParamStatements ? psScan
                        : prepareBaseline("SELECT sum(data_value) FROM blocking_data");
                double sum = 0;
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) sum = rs.getDouble(1); }
                if (!o.cachedParamStatements) ps.close();
                if (!o.autocommitReads) con.commit();
                out.println("TransectionSpec " + t.index + " FULL_SCAN sum=" + sum);
                logDone(t);
            } finally {
                if (o.autocommitReads) con.setAutoCommit(false);
            }
        }

        /** 1a-c) insert one new tuple; id from an atomic counter -> never a duplicate key. */
        void insert(TransectionSpec t) throws SQLException {
            int id = nextInsertId.getAndIncrement();
            PreparedStatement ps = o.cachedParamStatements ? psInsert
                    : prepareBaseline("INSERT INTO blocking_data VALUES (?, ?, " + t.index + ")");
            ps.setInt(1, id);
            ps.setDouble(2, t.values[0]);
            if (o.cachedParamStatements) ps.setInt(3, (int) t.index);
            ps.executeUpdate();
            if (!o.cachedParamStatements) ps.close();
            con.commit();
            logDone(t);
        }

        /** 1a-e) 3 update statements, each modifies exactly ONE tuple (primary key). Order is never changed. */
        void write(TransectionSpec t) throws SQLException {
            PreparedStatement ps = o.cachedParamStatements ? psUpdate
                    // Homework 2 style: task id glued into the SQL text -> a different SQL string per transaction
                    : prepareBaseline("UPDATE blocking_data SET data_value = ?, modified_by = " + t.index + " WHERE data_id = ?");
            for (int q = 0; q < t.ids.length; q++) {
                ps.setDouble(1, t.values[q]);
                if (o.cachedParamStatements) {
                    ps.setInt(2, (int) t.index);
                    ps.setInt(3, t.ids[q]);
                }
                else {
                    ps.setInt(2, t.ids[q]);
                }
                ps.executeUpdate();
            }
            if (!o.cachedParamStatements) ps.close();
            con.commit();
            logDone(t);
        }

        void logDone(TransectionSpec t) {
            if (!o.quietBufferedOutput) {
                System.out.println("Done task " + t.index + " (" + t.type + "). I am finisher number " + (stats.committed.get() + 1));
            }
        }
    }

    // ------------------------------------------------------------------
    // Output stream: baseline = System.out (flushes every line),
    // optimized = 64 KB buffer, flushed once per second / at the end.
    // ------------------------------------------------------------------
    static PrintStream outputFor(Options o) {
        if (!o.quietBufferedOutput) return System.out;
        return new PrintStream(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), 1 << 16), false);
    }

    static final class Result {
        String name; int threads; long committed; long retries; double seconds;
        double tps() { return committed / seconds; }
    }

    static List<Worker> openWorkers(Options o, Stats s, PrintStream out, int threads) throws SQLException {
        List<Worker> ws = new ArrayList<>();
        for (int i = 0; i < threads; i++) ws.add(new Worker(o, s, out));   // connection creation not measured
        return ws;
    }

    // ------------------------------------------------------------------
    // 2a / 2c / 2d) A FIXED stream of numTx transactions (tx 0..numTx-1).
    // Each worker takes the next index from a shared counter -> same set of
    // transactions for serial (threads = 1) and concurrent execution.
    // ------------------------------------------------------------------
    static Result runFixedStream(Options o, int threads, long numTx) throws Exception {
        createDB(o);
        Stats s = new Stats();
        PrintStream out = outputFor(o);
        List<Worker> workers = openWorkers(o, s, out, threads);
        AtomicLong next = new AtomicLong(0);
        CountDownLatch startSignal = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (Worker w : workers) {
            Thread th = new Thread(() -> {
                try { startSignal.await(); } catch (InterruptedException e) { return; }
                long i;
                while ((i = next.getAndIncrement()) < numTx) w.execute(generate(i));
            });
            th.start(); ts.add(th);
        }
        long start = System.nanoTime();
        startSignal.countDown();
        for (Thread th : ts) th.join();
        out.flush();
        long stop = System.nanoTime();
        workers.forEach(Worker::close);

        Result r = new Result();
        r.name = o.name; r.threads = threads; r.committed = s.committed.get();
        r.retries = s.retries.get(); r.seconds = (stop - start) / 1e9;
        return r;
    }

    // ------------------------------------------------------------------
    // 1b) CONTINUOUS stream for a given duration.
    // Closed system: every worker restarts with a NEW transaction as soon as its
    // previous one committed. Arrivals == commits -> equilibrium; the number of
    // transactions in the system is always = number of threads.
    // ------------------------------------------------------------------
    static Result runForDuration(Options o, int threads, int seconds, boolean liveTicker) throws Exception {
        createDB(o);
        Stats s = new Stats();
        PrintStream out = outputFor(o);
        List<Worker> workers = openWorkers(o, s, out, threads);
        AtomicLong next = new AtomicLong(0);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch startSignal = new CountDownLatch(1);
        List<Thread> ts = new ArrayList<>();
        for (Worker w : workers) {
            Thread th = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    return;
                }
                while (running.get()) w.execute(generate(next.getAndIncrement()));  // restart = next transaction
            });
            th.start(); ts.add(th);
        }
        long start = System.nanoTime();
        startSignal.countDown();
        long lastCommitted = 0;
        for (int sec = 1; sec <= seconds; sec++) {
            Thread.sleep(1000);
            if (o.quietBufferedOutput) out.flush();
            long c = s.committed.get();
            if (liveTicker) {
                System.err.printf("[t=%2ds] committed this second: %6d | total: %8d | started: %8d | in flight: %2d | retries: %d%n",
                        sec, c - lastCommitted, c, next.get(), s.inFlight.get(), s.retries.get());
            }
            lastCommitted = c;
        }
        running.set(false);
        for (Thread th : ts) th.join();          // running transactions are allowed to finish (commit)
        out.flush();
        long stop = System.nanoTime();
        workers.forEach(Worker::close);

        Result r = new Result();
        r.name = o.name; r.threads = threads; r.committed = s.committed.get();
        r.retries = s.retries.get(); r.seconds = (stop - start) / 1e9;
        if (liveTicker) {
            System.err.printf("Per type: MULTI_POINT_READ=%d INSERT=%d FULL_SCAN=%d WRITE=%d%n",
                    s.perType[0].get(), s.perType[1].get(), s.perType[2].get(), s.perType[3].get());
        }
        return r;
    }

    static void printHeader() {
        System.err.printf("%-28s %8s %12s %10s %10s %12s%n", "run", "threads", "committed", "retries", "time[s]", "tx/s");
    }

    static void print(Result r) {
        System.err.printf("%-28s %8d %12d %10d %10.2f %12.1f%n", r.name, r.threads, r.committed, r.retries, r.seconds, r.tps());
    }

    // ------------------------------------------------------------------
    public static void main(String[] args) throws Exception {
        String mode = args.length > 0 ? args[0] : "demo";
        int cores = Runtime.getRuntime().availableProcessors();

        switch (mode) {
            case "demo": {                                            // 1b + 1d
                int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 30;
                int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
                Options o = (args.length > 3 && args[3].equals("optimized")) ? Options.optimized() : Options.baseline();
                System.err.println("Live demo: " + seconds + " s, " + threads + " threads, " + o.name);
                Result r = runForDuration(o, threads, seconds, true);
                printHeader(); print(r);
                break;
            }
            case "sweep": {                                           // 1c
                int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 10;
                Options o = (args.length > 2 && args[2].equals("optimized")) ? Options.optimized() : Options.baseline();
                System.err.println("CPU cores (hardware threads) on this machine: " + cores);
                printHeader();
                double base = 0;
                for (int t : new int[]{1, 2, 4, 8, 16, 32}) {
                    Result r = runForDuration(o, t, seconds, false);
                    r.name = o.name + " (" + t + " thr)";
                    print(r);
                    if (t == 1) base = r.tps();
                    System.err.printf("      speed-up vs. 1 thread: %.2fx  (linear would be %dx)%n", r.tps() / base, t);
                }
                break;
            }
            case "serial": {                                          // 2a
                long n = args.length > 1 ? Long.parseLong(args[1]) : 10000;
                Options o = Options.baseline().copy("serial");
                printHeader(); print(runFixedStream(o, 1, n));
                break;
            }
            case "concurrent": {                                      // 2a (concurrent counterpart)
                long n = args.length > 1 ? Long.parseLong(args[1]) : 10000;
                int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
                printHeader(); print(runFixedStream(Options.baseline().copy("concurrent baseline"), threads, n));
                break;
            }
            case "optimized": {                                       // 2c
                long n = args.length > 1 ? Long.parseLong(args[1]) : 10000;
                int threads = args.length > 2 ? Integer.parseInt(args[2]) : cores;
                printHeader(); print(runFixedStream(Options.optimized(), threads, n));
                break;
            }
            case "compare": {                                         // 2d
                long n = args.length > 1 ? Long.parseLong(args[1]) : 10000;
                int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
                printHeader();
                Result serial = runFixedStream(Options.baseline().copy("serial (baseline)"), 1, n);         print(serial);
                Result serialOpt = runFixedStream(Options.optimized().copy("serial (optimized)"), 1, n);   print(serialOpt);
                Result conc = runFixedStream(Options.baseline().copy("concurrent (baseline)"), threads, n); print(conc);
                Result opt = runFixedStream(Options.optimized().copy("concurrent (optimized)"), threads, n); print(opt);
                System.err.printf("Speed-up vs. serial: concurrent baseline %.2fx, optimized %.2fx%n",
                        conc.tps() / serial.tps(), opt.tps() / serial.tps());
                break;
            }
            case "ablation": {                                        // 2b/2c: effect of every single fix
                long n = args.length > 1 ? Long.parseLong(args[1]) : 10000;
                int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
                Options o = Options.baseline().copy("0 baseline");
                printHeader();
                print(runFixedStream(o, threads, n));
                o = o.copy("1 +cached param statements"); o.cachedParamStatements = true; print(runFixedStream(o, threads, n));
                o = o.copy("2 +quiet/buffered output");   o.quietBufferedOutput = true;   print(runFixedStream(o, threads, n));
                o = o.copy("3 +autocommit reads");        o.autocommitReads = true;       print(runFixedStream(o, threads, n));
                o = o.copy("4 +synchronous_commit=off");  o.asyncCommit = true;           print(runFixedStream(o, threads, n));
                o = o.copy("5 +fillfactor 80 (HOT)");     o.hotFillfactor = true;         print(runFixedStream(o, threads, n));
                o = o.copy("6 +fast retry");              o.fastRetry = true;             print(runFixedStream(o, threads, n));
                o = o.copy("7 +index on data_value");     o.coveringIndex = true;         print(runFixedStream(o, threads, n));
                break;
            }
            default:
                System.err.println("Unknown mode " + mode + ". Modes: demo, sweep, serial, concurrent, optimized, compare, ablation");
        }
    }
}





//mvn compile exec:java -Dexec.mainClass=Homework3 -Dexec.args="load"                # only (re)create + load the 50 000 tuples
//mvn compile exec:java -Dexec.mainClass=Homework3 -Dexec.args="demo 4 30"           # 1b + 1d: demo <threads> <seconds>
//mvn compile exec:java -Dexec.mainClass=Homework3 -Dexec.args="demo 4 30 optimized" # same demo, optimized version
//mvn compile exec:java -Dexec.mainClass=Homework3 -Dexec.args="sweep 10"            # 1c: sweep <seconds per thread count>
//mvn compile exec:java -Dexec.mainClass=Homework3 -Dexec.args="sweep 10 optimized"  # 1c, optimized version
//mvn compile exec:java -Dexec.mainClass=Homework3 -Dexec.args="compare 2000 4"      # 2a + 2d: compare <numTx> <threads>
//mvn compile exec:java -Dexec.mainClass=Homework3 -Dexec.args="ablation 2000 4"     # 2c: effect of each fix

