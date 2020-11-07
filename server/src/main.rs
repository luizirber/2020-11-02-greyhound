use std::path::Path;
use std::sync::Arc;

use greyhound_core::RevIndex;
use sourmash::signature::Signature;
use sourmash::sketch::Sketch;
use tide::{Body, Request};

#[derive(Clone)]
struct RevIndexState {
    revindex: Arc<RevIndex>,
}

impl RevIndexState {
    fn load<P: AsRef<Path>>(path: P) -> Self {
        let revindex = RevIndex::load(path, None).expect("Error loading index");
        Self {
            revindex: Arc::new(revindex),
        }
    }

    fn gather(&self, query: Signature) -> Vec<String> {
        if let Some(sketch) = query.select_sketch(&self.revindex.template()) {
            if let Sketch::MinHash(mh) = sketch {
                let counter = self.revindex.counter_for_query(&mh);
                self.revindex.gather(counter, 0)
            } else {
                todo!("Return error")
            }
        } else {
            todo!("Return error")
        }
    }
}

#[async_std::main]
async fn main() -> tide::Result<()> {
    tide::log::start();
    let path = "data/genbank_bacteria.json.gz";
    let mut app = tide::with_state(RevIndexState::load(path));

    app.at("/submit")
        .post(|mut req: Request<RevIndexState>| async move {
            let raw_sig = req.body_bytes().await?;
            let sig = Signature::from_reader(&raw_sig[..])
                .expect("Error loading sig")
                .swap_remove(0);

            let result = req.state().gather(sig);

            Ok(Body::from_json(&result)?)
        });

    app.listen("127.0.0.1:8080").await?;
    Ok(())
}
