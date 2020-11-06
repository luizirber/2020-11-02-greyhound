use std::path::Path;
use std::sync::Arc;

//use tide::prelude::*;
use tide::{Body, Request, Response, StatusCode};

use greyhound_core::RevIndex;

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

    fn gather(&self, query: Sketch) -> Vec<String> {
        self.revindex.gather()
    }
}

#[async_std::main]
async fn main() -> tide::Result<()> {
    tide::log::start();
    let path = "";
    let mut app = tide::with_state(RevIndexState::load(path));

    app.at("/submit")
        .post(|mut req: Request<RevIndexState>| async move {
            let sig = req.body_bytes().await?;
            let result = req.state().gather(sig);

            Ok(Body::from_json(&result))
        });

    app.listen("127.0.0.1:8080").await?;
    Ok(())
}
