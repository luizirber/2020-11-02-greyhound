use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use log::info;
use nohash_hasher::BuildNoHashHasher;
use rayon::prelude::*;
use sourmash::signature::{Signature, SigsTrait};
use sourmash::sketch::minhash::{max_hash_for_scaled, KmerMinHash};
use sourmash::sketch::Sketch;
use structopt::StructOpt;

type RevIndex = HashMap<u64, HashSet<usize>, BuildNoHashHasher<u64>>;
type SigCounter = counter::Counter<usize>;

#[derive(StructOpt, Debug)]
enum Cli {
    Gather {
        /// Query signature
        #[structopt(parse(from_os_str))]
        query_path: PathBuf,

        /// List of reference signatures
        #[structopt(parse(from_os_str))]
        siglist: PathBuf,

        /// ksize
        #[structopt(short = "k", long = "ksize", default_value = "31")]
        ksize: u8,

        /// scaled
        #[structopt(short = "s", long = "scaled", default_value = "1000")]
        scaled: usize,

        /// threshold_bp
        #[structopt(short = "t", long = "threshold_bp", default_value = "50000")]
        threshold_bp: usize,

        /// The path for output
        #[structopt(parse(from_os_str), short = "o", long = "output")]
        output: Option<PathBuf>,

        /// Is the index a list of signatures?
        #[structopt(parse(from_os_str), short = "i", long = "index")]
        index: Option<PathBuf>,
    },
    Index {
        /// The path for output
        #[structopt(parse(from_os_str))]
        output: PathBuf,

        /// List of reference signatures
        #[structopt(parse(from_os_str))]
        siglist: PathBuf,

        /// ksize
        #[structopt(short = "k", long = "ksize", default_value = "31")]
        ksize: u8,

        /// scaled
        #[structopt(short = "s", long = "scaled", default_value = "1000")]
        scaled: usize,
    },
}

fn load_revindex<P: AsRef<Path>>(
    index_path: P,
    query: Option<&KmerMinHash>,
) -> Result<RevIndex, Box<dyn std::error::Error>> {
    // TODO: avoid loading full revindex if query != None
    let (rdr, _) = niffler::from_path(index_path)?;
    let mut revindex: RevIndex = serde_json::from_reader(rdr)?;

    if let Some(q) = query {
        let hashes: HashSet<u64> = q.iter_mins().cloned().collect();
        revindex.retain(|hash, _| hashes.contains(hash));
    }
    Ok(revindex)
}

fn build_revindex(
    search_sigs: &[PathBuf],
    template: &Sketch,
    threshold: usize,
    query: Option<&KmerMinHash>,
) -> RevIndex {
    let processed_sigs = AtomicUsize::new(0);

    search_sigs
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

            let (matched_hashes, intersection) = if let Some(q) = query {
                q.intersection(search_mh).unwrap()
            } else {
                let matched = search_mh.mins();
                let size = matched.len() as u64;
                (matched, size)
            };

            if matched_hashes.is_empty() || intersection < threshold as u64 {
                None
            } else {
                let mut revindex: RevIndex = HashMap::with_hasher(BuildNoHashHasher::default());
                matched_hashes.into_iter().for_each(|hash| {
                    let mut dataset_ids = HashSet::new();
                    dataset_ids.insert(dataset_id);
                    revindex.insert(hash, dataset_ids);
                });
                Some(revindex)
            }
        })
        .reduce(
            || HashMap::with_hasher(BuildNoHashHasher::default()),
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
        )
}

fn build_counter(revindex: &RevIndex, query: Option<&KmerMinHash>) -> SigCounter {
    if let Some(q) = query {
        let hashes: HashSet<u64> = q.iter_mins().cloned().collect();
        revindex
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
            .iter()
            .map(|(_, ids)| ids)
            .flatten()
            .cloned()
            .collect()
    }
}

fn index<P: AsRef<Path>>(
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

fn gather<P: AsRef<Path>>(
    query_path: P,
    siglist: P,
    ksize: u8,
    scaled: usize,
    threshold_bp: usize,
    output: Option<P>,
    index: Option<P>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Loading queries");

    let max_hash = max_hash_for_scaled(scaled as u64);
    let template_mh = KmerMinHash::builder()
        .num(0u32)
        .ksize(ksize as u32)
        .max_hash(max_hash)
        .build();
    let template = Sketch::MinHash(template_mh);

    let query_sig = Signature::from_path(query_path)?;
    let mut query = None;
    for sig in &query_sig {
        if let Some(sketch) = sig.select_sketch(&template) {
            if let Sketch::MinHash(mh) = sketch {
                query = Some((sig.name(), mh.clone()));
            }
        }
    }

    if query.is_none() {
        todo!("throw error, couldn't find matching sig")
    };
    let query = query.unwrap();

    let threshold = threshold_bp / (query.1.size() * scaled);

    info!("Loaded query signature {}", query.0);

    info!("Loading siglist");
    let siglist_file = BufReader::new(File::open(siglist)?);
    let search_sigs: Vec<PathBuf> = siglist_file
        .lines()
        .map(|line| {
            let mut path = PathBuf::new();
            path.push(line.unwrap());
            path
        })
        .collect();
    info!("Loaded {} sig paths in siglist", search_sigs.len());

    // Step 1: filter and prepare a reduced RevIndex and Counter
    let revindex = if let Some(index) = index {
        load_revindex(index, Some(&query.1))?
    } else {
        build_revindex(&search_sigs, &template, threshold, Some(&query.1))
    };
    let mut counter = build_counter(&revindex, None);

    // Step 2: Gather using the RevIndex and Counter
    let mut match_size = usize::max_value();
    let mut matches = vec![];

    while match_size > threshold && !counter.is_empty() {
        let (dataset_id, size) = counter.most_common()[0];
        match_size = if size >= threshold { size } else { break };

        let mut match_mh = None;
        let match_path = &search_sigs[dataset_id];
        let match_sig = &Signature::from_path(&match_path)
            .unwrap_or_else(|_| panic!("Error processing {:?}", match_path))[0];
        if let Some(sketch) = match_sig.select_sketch(&template) {
            if let Sketch::MinHash(mh) = sketch {
                match_mh = Some(mh);
            }
        }
        let match_mh = match_mh.unwrap();
        matches.push(match_sig.clone());

        for hash in match_mh.iter_mins() {
            if let Some(dataset_ids) = revindex.get(hash) {
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

    let out: Box<dyn Write + Send> = if let Some(path) = output {
        Box::new(BufWriter::new(File::create(path).unwrap()))
    } else {
        Box::new(std::io::stdout())
    };
    serde_json::to_writer(out, &matches)?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match Cli::from_args() {
        Cli::Gather {
            query_path,
            siglist,
            ksize,
            scaled,
            threshold_bp,
            output,
            index,
        } => gather(
            query_path,
            siglist,
            ksize,
            scaled,
            threshold_bp,
            output,
            index,
        )?,
        Cli::Index {
            output,
            siglist,
            ksize,
            scaled,
        } => index(siglist, ksize, scaled, output)?,
    };

    Ok(())
}
