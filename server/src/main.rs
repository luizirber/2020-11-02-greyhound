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

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Signature is not compatible with index")]
    UnsupportedSignature,

    #[error("Sketch is not compatible with index")]
    UnsupportedSketch,

    #[error("Couldn't load the index ({0})")]
    IndexLoading(String),

    #[error("Error during gather ({0})")]
    Gather(String),
}

impl RevIndexState {
    fn load<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let revindex =
            RevIndex::load(path, None).map_err(|e| Error::IndexLoading(format!("{}", e)))?;
        Ok(Self {
            revindex: Arc::new(revindex),
        })
    }

    fn gather(&self, query: Signature) -> Result<Vec<String>, Error> {
        if let Some(sketch) = query.select_sketch(&self.revindex.template()) {
            if let Sketch::MinHash(mh) = sketch {
                let counter = self.revindex.counter_for_query(&mh);
                Ok(self
                    .revindex
                    .gather(counter, 0)
                    .map_err(|e| Error::Gather(format!("{}", e)))?)
            } else {
                Err(Error::UnsupportedSketch)
            }
        } else {
            Err(Error::UnsupportedSignature)
        }
    }
}

#[async_std::main]
async fn main() -> tide::Result<()> {
    tide::log::start();
    let path = "data/genbank_bacteria.json.gz";
    let mut app = tide::with_state(RevIndexState::load(path)?);

    app.at("/submit")
        .post(|mut req: Request<RevIndexState>| async move {
            let raw_sig = req.body_bytes().await?;
            let sig = Signature::from_reader(&raw_sig[..])
                .expect("Error loading sig")
                .swap_remove(0);

            let result = req.state().gather(sig)?;

            Ok(Body::from_json(&result)?)
        });

    app.listen("127.0.0.1:8080").await?;
    Ok(())
}
