use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

/// A cache that gets persisted to disk for the true cardinalities of queries on a DBMS
/// It's difficult to share the same cache (i.e. having a three-level hash map of DBMS -> db -> sql ->
///   truecard) between multiple DBMSs because then multiple different Rust objects could be writing to
///   the same cache file.
/// It's possible to have one cache per database per DBMS instead of one per DBMS, but it just felt
///   cleaner to me to tie the lifetime of the cache with the lifetime of the DBMS object. If you
///   did one cache per database per DBMS, those caches would still be tied to the lifetime of the
///   DBMS object.
/// Note that only the dbms_cache field gets persisted
pub struct DBMSTruecardCache {
    workspace_dpath: PathBuf,
    // The DBMS the queries of this cache were executed on
    dbms_name: String,
    // The cache from dbname -> sql -> true cardinality
    // Note that dbname is the database _within_ the DBMS that sql was executed on
    dbms_cache: HashMap<String, HashMap<String, usize>>,
}

impl DBMSTruecardCache {
    fn get_serialized_fpath<P: AsRef<Path>>(workspace_dpath: P, dbms_name: &str) -> PathBuf {
        workspace_dpath
            .as_ref()
            .join("truecard_caches")
            .join(dbms_name)
    }

    pub fn build<P: AsRef<Path>>(workspace_dpath: P, dbms_name: &str) -> anyhow::Result<Self> {
        let serialized_fpath = Self::get_serialized_fpath(&workspace_dpath, dbms_name);
        let dbms_cache = if serialized_fpath.exists() {
            let file = File::open(serialized_fpath)?;
            serde_json::from_reader(file)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            workspace_dpath: PathBuf::from(workspace_dpath.as_ref()),
            dbms_name: String::from(dbms_name),
            dbms_cache,
        })
    }

    pub fn insert_truecard(&mut self, dbname: &str, sql: &str, truecard: usize) {
        let db_cache = match self.dbms_cache.get_mut(dbname) {
            Some(db_cache) => db_cache,
            None => {
                self.dbms_cache.insert(String::from(dbname), HashMap::new());
                self.dbms_cache.get_mut(dbname).unwrap()
            }
        };
        db_cache.insert(String::from(sql), truecard);
    }

    pub fn get_truecard(&self, dbname: &str, sql: &str) -> Option<usize> {
        self.dbms_cache
            .get(dbname)
            .and_then(|db_cache| db_cache.get(sql).copied())
    }

    pub fn save(&self) -> anyhow::Result<()> {
        let serialized_fpath = Self::get_serialized_fpath(&self.workspace_dpath, &self.dbms_name);
        fs::create_dir_all(serialized_fpath.parent().unwrap())?;
        // this will create a new file or truncate the file if it already exists
        let file = File::create(serialized_fpath)?;
        serde_json::to_writer(file, &self.dbms_cache)?;
        Ok(())
    }
}

impl Drop for DBMSTruecardCache {
    fn drop(&mut self) {
        self.save().unwrap();
    }
}
