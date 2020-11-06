use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use log::info;
use nohash_hasher::BuildNoHashHasher;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use sourmash::signature::{Signature, SigsTrait};
use sourmash::sketch::minhash::{max_hash_for_scaled, KmerMinHash};
use sourmash::sketch::Sketch;

type HashToIdx = HashMap<u64, HashSet<usize>, BuildNoHashHasher<u64>>;

#[derive(Serialize, Deserialize)]
struct RevIndex {
    hash_to_idx: HashToIdx,
    sig_files: Vec<PathBuf>,
}

type SigCounter = counter::Counter<usize>;

fn load_revindex<P: AsRef<Path>>(
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

fn build_revindex(
    search_sigs: &[PathBuf],
    template: &Sketch,
    threshold: usize,
    queries: Option<&[KmerMinHash]>,
) -> RevIndex {
    let processed_sigs = AtomicUsize::new(0);

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
            if let Some(qs) = queries {
                for query in qs {
                    let (matched_hashes, intersection) = query.intersection(search_mh).unwrap();
                    if !matched_hashes.is_empty() || intersection > threshold as u64 {
                        matched_hashes.into_iter().for_each(|hash| {
                            let mut dataset_ids = HashSet::new();
                            dataset_ids.insert(dataset_id);
                            hash_to_idx.insert(hash, dataset_ids);
                        });
                    }
                }
            } else {
                let matched = search_mh.mins();
                let size = matched.len() as u64;
                if !matched.is_empty() || size > threshold as u64 {
                    matched.into_iter().for_each(|hash| {
                        let mut dataset_ids = HashSet::new();
                        dataset_ids.insert(dataset_id);
                        hash_to_idx.insert(hash, dataset_ids);
                    });
                }
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
    RevIndex {
        hash_to_idx,
        sig_files: search_sigs.into(),
    }
}

fn build_counter(revindex: &RevIndex, query: Option<&KmerMinHash>) -> SigCounter {
    if let Some(q) = query {
        let hashes: HashSet<u64> = q.iter_mins().cloned().collect();
        revindex
            .hash_to_idx
            .iter()
            .filter_map(|(hash, ids)| {
                if hashes.contains(hash) {
                    Some(ids)
                } else {
                    None
                }
            })
            .flatten()
            .cloned()
            .collect()
    } else {
        revindex
            .hash_to_idx
            .iter()
            .map(|(_, ids)| ids)
            .flatten()
            .cloned()
            .collect()
    }
}

pub fn index<P: AsRef<Path>>(
    siglist: P,
    ksize: u8,
    scaled: usize,
    output: P,
) -> Result<(), Box<dyn std::error::Error>> {
    let max_hash = max_hash_for_scaled(scaled as u64);
    let template_mh = KmerMinHash::builder()
        .num(0u32)
        .ksize(ksize as u32)
        .max_hash(max_hash)
        .build();
    let template = Sketch::MinHash(template_mh);

    info!("Loading siglist");
    let siglist_file = BufReader::new(File::open(siglist)?);
    let index_sigs: Vec<PathBuf> = siglist_file
        .lines()
        .map(|line| {
            let mut path = PathBuf::new();
            path.push(line.unwrap());
            path
        })
        .collect();
    info!("Loaded {} sig paths in siglist", index_sigs.len());

    let revindex = build_revindex(&index_sigs, &template, 0, None);

    info!("Saving index");
    let wtr = niffler::to_path(
        output,
        niffler::compression::Format::Gzip,
        niffler::compression::Level::One,
    )?;
    serde_json::to_writer(wtr, &revindex)?;

    Ok(())
}

fn read_paths<P: AsRef<Path>>(paths_file: P) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let paths = BufReader::new(File::open(paths_file)?);
    Ok(paths
        .lines()
        .map(|line| {
            let mut path = PathBuf::new();
            path.push(line.unwrap());
            path
        })
        .collect())
}

fn build_template(ksize: u8, scaled: usize) -> Sketch {
    let max_hash = max_hash_for_scaled(scaled as u64);
    let template_mh = KmerMinHash::builder()
        .num(0u32)
        .ksize(ksize as u32)
        .max_hash(max_hash)
        .build();
    Sketch::MinHash(template_mh)
}

pub fn gather<P: AsRef<Path>>(
    queries_file: P,
    siglist: P,
    ksize: u8,
    scaled: usize,
    threshold_bp: usize,
    output: Option<P>,
    from_file: bool,
    lazy: bool,
    preload: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Loading queries");

    let template = build_template(ksize, scaled);
    let queries_path = read_paths(queries_file)?;

    let mut queries = vec![];
    let mut threshold = usize::max_value();
    if !lazy || from_file {
        for query_path in &queries_path {
            let query_sig = Signature::from_path(query_path)?;
            let mut query = None;
            for sig in &query_sig {
                if let Some(sketch) = sig.select_sketch(&template) {
                    if let Sketch::MinHash(mh) = sketch {
                        query = Some(mh.clone());
                        // TODO: deal with mh.size() == 0
                        let t = threshold_bp / (mh.size() * scaled);
                        threshold = cmp::min(threshold, t);
                    }
                }
            }
            if let Some(q) = query {
                queries.push(q);
            } else {
                todo!("throw error, some sigs were not valid")
            };
        }
    }

    info!("Loaded {} query signatures", queries_path.len());

    // Step 1: filter and prepare a reduced RevIndex for all queries
    let revindex = if from_file {
        info!("Loading siglist");
        let search_sigs = read_paths(siglist)?;
        info!("Loaded {} sig paths in siglist", search_sigs.len());

        build_revindex(&search_sigs, &template, threshold, Some(&queries))
    } else {
        if lazy {
            load_revindex(siglist, None)
        } else {
            load_revindex(siglist, Some(&queries))
        }?
    };

    let refsigs = if preload {
        revindex
            .sig_files
            .par_iter()
            .map(|ref_path| {
                Signature::from_path(&ref_path)
                    .unwrap_or_else(|_| panic!("Error processing {:?}", ref_path))
                    .swap_remove(0)
            })
            .collect()
    } else {
        vec![]
    };

    let outdir: PathBuf = if let Some(p) = output {
        p.as_ref().into()
    } else {
        let mut path = PathBuf::new();
        path.push("outputs");
        path
    };
    std::fs::create_dir_all(&outdir)?;

    // Step 2: Gather using the RevIndex and a specific Counter for each query
    queries_path.par_iter().enumerate().for_each(|(i, query)| {
        let query = if lazy {
            let query_sig = Signature::from_path(query).unwrap();
            let mut query = None;
            for sig in &query_sig {
                if let Some(sketch) = sig.select_sketch(&template) {
                    if let Sketch::MinHash(mh) = sketch {
                        if mh.size() == 0 {
                            return;
                        }
                        query = Some(mh.clone());
                    }
                }
            }
            query.unwrap()
        } else {
            queries[i].clone()
        };

        info!("Build counter for query");
        let mut counter = build_counter(&revindex, Some(&query));
        let threshold = threshold_bp / (query.size() * scaled);

        info!("Starting gather");
        let mut match_size = usize::max_value();
        let mut matches = vec![];

        while match_size > threshold && !counter.is_empty() {
            let (dataset_id, size) = counter.most_common()[0];
            match_size = if size >= threshold { size } else { break };

            let ref_match;
            let match_sig = if preload {
                &refsigs[dataset_id]
            } else {
                let match_path = &revindex.sig_files[dataset_id];
                ref_match = Signature::from_path(&match_path)
                    .unwrap_or_else(|_| panic!("Error processing {:?}", match_path))
                    .swap_remove(0);
                &ref_match
            };

            let mut match_mh = None;
            if let Some(sketch) = match_sig.select_sketch(&template) {
                if let Sketch::MinHash(mh) = sketch {
                    match_mh = Some(mh);
                }
            }
            let match_mh = match_mh.unwrap();
            matches.push(&revindex.sig_files[dataset_id]);

            for hash in match_mh.iter_mins() {
                if let Some(dataset_ids) = revindex.hash_to_idx.get(hash) {
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

        info!("Saving {} matches", matches.len());
        let mut path = outdir.clone();
        path.push(queries_path[i].file_name().unwrap());

        let mut out = BufWriter::new(File::create(path).unwrap());
        for m in matches {
            writeln!(out, "{}", m.to_str().unwrap()).unwrap();
        }
        info!("Finishing query {:?}", queries_path[i]);
    });

    info!("Finished");
    Ok(())
}
