// Copyright (c) 2023-2024 CMU Database Group
//
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file or at
// https://opensource.org/licenses/MIT.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};

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

    pub fn insert_truecard(
        &mut self,
        data_and_queries_name: &str,
        query_id: &str,
        truecard: usize,
    ) {
        let db_cache = match self.cache.get_mut(data_and_queries_name) {
            Some(db_cache) => db_cache,
            None => {
                self.cache
                    .insert(String::from(data_and_queries_name), HashMap::new());
                self.cache.get_mut(data_and_queries_name).unwrap()
            }
        };
        db_cache.insert(String::from(query_id), truecard);
    }

    pub fn get_truecard(&self, data_and_queries_name: &str, query_id: &str) -> Option<usize> {
        self.cache
            .get(data_and_queries_name)
            .and_then(|db_cache| db_cache.get(query_id).copied())
    }

    pub fn save(&self) -> anyhow::Result<()> {
        fs::create_dir_all(self.truecard_cache_fpath.parent().unwrap())?;
        // this will create a new file or truncate the file if it already exists
        let file = File::create(&self.truecard_cache_fpath)?;
        serde_json::to_writer_pretty(file, &self.cache)?;
        Ok(())
    }
}

impl Drop for TruecardCache {
    fn drop(&mut self) {
        self.save().unwrap();
    }
}
