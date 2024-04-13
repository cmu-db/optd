use std::{
    collections::HashMap,
    fs::{self, File},
    path::{Path, PathBuf},
};

use async_trait::async_trait;

use crate::benchmark::Benchmark;

#[async_trait]
pub trait TruecardGetter {
    async fn get_benchmark_truecards(
        &mut self,
        benchmark: &Benchmark,
    ) -> anyhow::Result<Vec<usize>>;
}

/// A cache that gets persisted to disk for the true cardinalities of all queries of all benchmarks
pub struct TruecardCache {
    truecard_cache_fpath: PathBuf,
    cache: HashMap<String, HashMap<String, usize>>,
}

impl TruecardCache {
    pub fn build<P: AsRef<Path>>(truecard_cache_fpath: P) -> anyhow::Result<Self> {
        let truecard_cache_fpath = PathBuf::from(truecard_cache_fpath.as_ref());
        let cache = if truecard_cache_fpath.exists() {
            let file = File::open(&truecard_cache_fpath)?;
            serde_json::from_reader(file)?
        } else {
            HashMap::new()
        };

        Ok(Self {
            truecard_cache_fpath,
            cache,
        })
    }

    pub fn insert_truecard(&mut self, dbname: &str, query_id: &str, truecard: usize) {
        let db_cache = match self.cache.get_mut(dbname) {
            Some(db_cache) => db_cache,
            None => {
                self.cache.insert(String::from(dbname), HashMap::new());
                self.cache.get_mut(dbname).unwrap()
            }
        };
        db_cache.insert(String::from(query_id), truecard);
    }

    pub fn get_truecard(&self, dbname: &str, query_id: &str) -> Option<usize> {
        self.cache
            .get(dbname)
            .and_then(|db_cache| db_cache.get(query_id).copied())
    }

    pub fn save(&self) -> anyhow::Result<()> {
        fs::create_dir_all(self.truecard_cache_fpath.parent().unwrap())?;
        // this will create a new file or truncate the file if it already exists
        let file = File::create(&self.truecard_cache_fpath)?;
        serde_json::to_writer(file, &self.cache)?;
        Ok(())
    }
}

impl Drop for TruecardCache {
    fn drop(&mut self) {
        self.save().unwrap();
    }
}
