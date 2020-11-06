use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use log::info;
use nohash_hasher::BuildNoHashHasher;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sourmash::signature::Signature;
use sourmash::sketch::minhash::KmerMinHash;
use sourmash::sketch::Sketch;

type HashToIdx = HashMap<u64, HashSet<usize>, BuildNoHashHasher<u64>>;
type SigCounter = counter::Counter<usize>;

#[derive(Serialize, Deserialize)]
pub struct RevIndex {
    hash_to_idx: HashToIdx,
    sig_files: Vec<PathBuf>,
    ref_sigs: Option<Vec<Signature>>,
    template: Sketch,
}

impl RevIndex {
    pub fn load<P: AsRef<Path>>(
        index_path: P,
        queries: Option<&[KmerMinHash]>,
    ) -> Result<RevIndex, Box<dyn std::error::Error>> {
        // TODO: avoid loading full revindex if query != None
        let (rdr, _) = niffler::from_path(index_path)?;
        let mut revindex: RevIndex = serde_json::from_reader(rdr)?;

        if let Some(qs) = queries {
            for q in qs {
                let hashes: HashSet<u64> = q.iter_mins().cloned().collect();
                revindex.hash_to_idx.retain(|hash, _| hashes.contains(hash));
            }
        }
        Ok(revindex)
    }

    pub fn new(
        search_sigs: &[PathBuf],
        template: &Sketch,
        threshold: usize,
        queries: Option<&[KmerMinHash]>,
        keep_sigs: bool,
    ) -> RevIndex {
        let processed_sigs = AtomicUsize::new(0);

        // If threshold is zero, let's merge all queries and save time later
        let merged_query = if let Some(qs) = queries {
            if threshold == 0 {
                let mut merged = qs[0].clone();
                for query in &qs[1..] {
                    merged.merge(query).unwrap();
                }
                Some(merged)
            } else {
                None
            }
        } else {
            None
        };

        let hash_to_idx = search_sigs
            .par_iter()
            .enumerate()
            .filter_map(|(dataset_id, filename)| {
                let i = processed_sigs.fetch_add(1, Ordering::SeqCst);
                if i % 1000 == 0 {
                    info!("Processed {} reference sigs", i);
                }

                let mut search_mh = None;
                let search_sig = &Signature::from_path(&filename)
                    .unwrap_or_else(|_| panic!("Error processing {:?}", filename))[0];
                if let Some(sketch) = search_sig.select_sketch(&template) {
                    if let Sketch::MinHash(mh) = sketch {
                        search_mh = Some(mh);
                    }
                }
                let search_mh = search_mh.unwrap();

                let mut hash_to_idx = HashToIdx::with_hasher(BuildNoHashHasher::default());
                let mut add_to = |matched_hashes: Vec<u64>, intersection| {
                    if !matched_hashes.is_empty() || intersection > threshold as u64 {
                        matched_hashes.into_iter().for_each(|hash| {
                            let mut dataset_ids = HashSet::new();
                            dataset_ids.insert(dataset_id);
                            hash_to_idx.insert(hash, dataset_ids);
                        });
                    }
                };

                if let Some(qs) = queries {
                    if let Some(ref merged) = merged_query {
                        let (matched_hashes, intersection) =
                            merged.intersection(search_mh).unwrap();
                        add_to(matched_hashes, intersection);
                    } else {
                        for query in qs {
                            let (matched_hashes, intersection) =
                                query.intersection(search_mh).unwrap();
                            add_to(matched_hashes, intersection);
                        }
                    }
                } else {
                    let matched = search_mh.mins();
                    let size = matched.len() as u64;
                    add_to(matched, size);
                };

                if hash_to_idx.is_empty() {
                    None
                } else {
                    Some(hash_to_idx)
                }
            })
            .reduce(
                || HashToIdx::with_hasher(BuildNoHashHasher::default()),
                |a, b| {
                    let (small, mut large) = if a.len() > b.len() { (b, a) } else { (a, b) };

                    small.into_iter().for_each(|(hash, ids)| {
                        let entry = large.entry(hash).or_insert_with(HashSet::new);
                        for id in ids {
                            entry.insert(id);
                        }
                    });

                    large
                },
            );

        // TODO: build this together with hash_to_idx?
        let ref_sigs = if keep_sigs {
            Some(
                search_sigs
                    .par_iter()
                    .map(|ref_path| {
                        Signature::from_path(&ref_path)
                            .unwrap_or_else(|_| panic!("Error processing {:?}", ref_path))
                            .swap_remove(0)
                    })
                    .collect(),
            )
        } else {
            None
        };

        RevIndex {
            hash_to_idx,
            sig_files: search_sigs.into(),
            ref_sigs,
            template: template.clone(),
        }
    }

    pub fn gather(&self, mut counter: SigCounter, threshold: usize) -> Vec<String> {
        let mut match_size = usize::max_value();
        let mut matches = vec![];

        while match_size > threshold && !counter.is_empty() {
            let (dataset_id, size) = counter.most_common()[0];
            match_size = if size >= threshold { size } else { break };

            let match_path = &self.sig_files[dataset_id];
            let ref_match;
            let match_sig = if let Some(refsigs) = &self.ref_sigs {
                &refsigs[dataset_id]
            } else {
                ref_match = Signature::from_path(&match_path)
                    .unwrap_or_else(|_| panic!("Error processing {:?}", match_path))
                    .swap_remove(0);
                &ref_match
            };

            let mut match_mh = None;
            if let Some(sketch) = match_sig.select_sketch(&self.template) {
                if let Sketch::MinHash(mh) = sketch {
                    match_mh = Some(mh);
                }
            }
            let match_mh = match_mh.unwrap();
            matches.push(match_path.to_str().unwrap().into());

            for hash in match_mh.iter_mins() {
                if let Some(dataset_ids) = self.hash_to_idx.get(hash) {
                    for dataset in dataset_ids {
                        counter.entry(*dataset).and_modify(|e| {
                            if *e > 0 {
                                *e -= 1
                            }
                        });
                    }
                }
            }
            counter.remove(&dataset_id);
        }
        matches
    }

    pub fn counter_for_query(&self, query: &KmerMinHash) -> SigCounter {
        query
            .iter_mins()
            .filter_map(|h| self.hash_to_idx.get(h))
            .flatten()
            .cloned()
            .collect()
    }

    pub fn counter(&self) -> SigCounter {
        self.hash_to_idx
            .iter()
            .map(|(_, ids)| ids)
            .flatten()
            .cloned()
            .collect()
    }
}
