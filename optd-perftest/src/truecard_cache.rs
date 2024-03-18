/// A cache that gets persisted to disk for the true cardinalities of queries on a DBMS
struct TruecardCache {
    // The DBMS the queries of this cache were executed on
    dbms_name: String,
    // The cache from dbname -> sql -> cardinality
    // Note that dbname is the database _within_ the DBMS that sql was executed on
    cache: HashMap<String, HashMap<String, usize>>,
}

// TODO(phw2): write function that takes in a Benchmark and creates a new entry in the cache
impl TruecardCache {
    
}