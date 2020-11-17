#![recursion_limit = "1024"]

pub mod native_worker;

use anyhow::Error;
use log::info;
use needletail::{parse_fastx_reader, Sequence};
use serde::{Deserialize, Serialize};
use web_sys::DragEvent;
use yew::format::{Json, Nothing};
use yew::services::fetch::{FetchService, FetchTask, Request, Response};
use yew::services::reader::{File, FileChunk, FileData, ReaderService, ReaderTask};
use yew::worker::{Bridge, Bridged};
use yew::{html, Callback, ChangeData, Component, ComponentLink, Html, ShouldRender};

use sourmash::cmd::ComputeParameters;
use sourmash::signature::Signature;

#[derive(Serialize, Deserialize, Debug)]
pub struct GatherResult {
    intersect_bp: usize,
    f_orig_query: f64,
    f_match: f64,
    f_unique_to_query: f64,
    f_unique_weighted: f64,
    average_abund: usize,
    median_abund: usize,
    std_abund: usize,
    filename: String,
    name: String,
    md5: String,
    match_: String,
    f_match_orig: f64,
    unique_intersect_bp: usize,
    gather_result_rank: usize,
    remaining_bp: usize,
}

pub struct Model {
    link: ComponentLink<Self>,
    job: Box<dyn Bridge<native_worker::Worker>>,
    reader: ReaderService,
    tasks: Vec<ReaderTask>,
    files: Vec<String>,
    ft: Option<FetchTask>,
    sig: Option<Signature>,
}

pub enum Msg {
    SendToWorker,
    DataReceived,
    Drop(DragEvent),
    Loaded(FileData),
    Chunk(Option<FileChunk>),
    Files(Vec<File>),
    FetchData(Vec<u8>),
    FetchReady(Result<Vec<GatherResult>, Error>),
    Ignore,
}

impl Component for Model {
    type Message = Msg;
    type Properties = ();

    fn create(_: Self::Properties, link: ComponentLink<Self>) -> Self {
        let callback = link.callback(|_| Msg::DataReceived);
        let job = native_worker::Worker::bridge(callback);

        Model {
            link,
            job,
            reader: ReaderService::new(),
            tasks: vec![],
            files: vec![],
            ft: None,
            sig: None,
        }
    }

    fn update(&mut self, msg: Self::Message) -> ShouldRender {
        match msg {
            Msg::SendToWorker => {
                self.job.send(native_worker::Request::GetDataFromServer);
            }
            Msg::DataReceived => {
                info!("DataReceived");
            }
            Msg::Drop(_) => unimplemented!(),
            Msg::FetchData(json) => {
                let callback = self.link.callback(
                    move |response: Response<Json<Result<Vec<GatherResult>, Error>>>| {
                        let (meta, Json(data)) = response.into_parts();
                        println!("META: {:?}, {:?}", meta, data);
                        if meta.status.is_success() {
                            Msg::FetchReady(data)
                        } else {
                            Msg::Ignore // FIXME: Handle this error accordingly.
                        }
                    },
                );
                let request = Request::post("/gather").body(Ok(json)).unwrap();
                self.ft = Some(FetchService::fetch_binary(request, callback).unwrap());
            }
            Msg::FetchReady(result) => {
                info!("{:?}", result);
                // result is Vec<GatherResult>
                //todo!("populate the table")
            }
            Msg::Loaded(file) => {
                let params = ComputeParameters::builder()
                    .ksizes(vec![21])
                    .num_hashes(0)
                    .scaled(2000)
                    .build();
                let mut sig = Signature::from_params(&params);

                let mut buf = vec![];
                let (mut reader, _) = niffler::get_reader(Box::new(&file.content[..])).unwrap();
                reader.read_to_end(&mut buf).unwrap();
                let mut parser = parse_fastx_reader(&buf[..]).unwrap();
                while let Some(record) = parser.next() {
                    let record = record.unwrap();
                    let norm_seq = record.normalize(true);
                    sig.add_sequence(&norm_seq, true).unwrap();
                }
                let json = serde_json::to_vec(&[&sig]).unwrap();
                self.sig = Some(sig);
                // info!("{:?}", &json);
                self.link.send_message(Msg::FetchData(json));
            }
            Msg::Chunk(Some(chunk)) => {
                let info = format!("chunk: {:?}", chunk);
                self.files.push(info);
            }
            Msg::Files(files) => {
                let chunks = false;
                for file in files.into_iter() {
                    let task = {
                        if chunks {
                            let callback = self.link.callback(Msg::Chunk);
                            self.reader.read_file_by_chunks(file, callback, 10).unwrap()
                        } else {
                            let callback = self.link.callback(Msg::Loaded);
                            self.reader.read_file(file, callback).unwrap()
                        }
                    };
                    self.tasks.push(task);
                }
            }
            _ => return false,
        }
        true
    }

    fn view(&self) -> Html {
        html! {
          <>
            <header>
              <h2>{"greyhound gather"}</h2>
            </header>

            <div class="columns">
              <div id="files" class="box" ondragover=Callback::from(|e: DragEvent| {e.prevent_default();})>
                <div id="drag-container" ondrop=self.link.callback(move |event: DragEvent| {
                  event.prevent_default();
                  event.stop_propagation();

                  //let dt = event.data_transfer().unwrap();
                  // let files = dt.items();
                  // let img = files.get(0).unwrap();
                  //
                  // let file_reader = web_sys::FileReader::new().unwrap();
                  // file_reader.read_as_data_url(&img).unwrap();
                  //let img = file_reader.result().unwrap();
                  //let img = File::new_with_buffer_source_sequence(&img, "tmp");

                  Msg::Drop(event)
                }) >
                  <p>{"Choose a FASTA/Q file to upload. File can be gzip-compressed."}</p>
                    <input type="file" multiple=true onchange=self.link.callback(move |value| {
                            let mut result = Vec::new();
                            if let ChangeData::Files(files) = value {
                                let files = js_sys::try_iter(&files)
                                    .unwrap()
                                    .unwrap()
                                    .into_iter()
                                    .map(|v| File::from(v.unwrap()));
                                result.extend(files);
                            }
                            Msg::Files(result)
                        })/>
                </div>

                <div id="progress-container">
                  <div id="progress-bar"></div>
                </div>
                <div class="columns">
                  <div class="box" id="download">
                    <button id="download_btn" type="button" disabled=true>{"Download"}</button>
                  </div>
                </div>

                <div id="results-container"></div>
              </div>

              <div id="info" class="box">
                <p>
                  {"This is a demo for a system running "}<b>{"gather"}</b>
                  {", an algorithm for decomposing a query into reference datasets."}
                </p>

                <p>
                  <b>{"greyhound"}</b>{" is an optimized approach for running "}<b>{"gather"}</b>
                  {" based on an Inverted Index containing a mapping of hashes to datasets containing them.
                  In this demo the datasets are Scaled MinHash sketches (k=21, scaled=2000)
                  calculated from the "}
                  <a href="https://gtdb.ecogenomic.org/stats">{"31,910 species clusters in the GTDB r95"}</a>{"."}
                </p>

                <p>
                  {"This demo server is hosted on a "}<a href="https://aws.amazon.com/ec2/instance-types/t3/">{"t3.2xlarge"}</a>
                  {" spot instance on AWS, using ~10GB of the RAM for the inverted index + signature caching (for speed).
                  The server is implemented using "}<a href="https://github.com/http-rs/tide">{"tide"}</a>{", "}
                  {"an async web framework written in "}<a href="https://rust-lang.org">{"Rust"}</a>{". "}
                  {"The frontend is implemented in JavaScript and "}<a href="https://webassembly.org/">{"WebAssembly"}</a>
                  {" for calculating the Scaled MinHash sketch in your browser,
                  instead of uploading the full data to the server.
                  This uses the Rust implementation of sourmash compiled to WebAssembly using "}
                  <a href="https://rustwasm.github.io/docs/wasm-bindgen/">{"wasm-bindgen"}</a>{" and packaged with "}
                  <a href="https://rustwasm.github.io/wasm-pack/">{"wasm-pack"}</a>{"."}
                </p>

                <p>
                  {"For more info about the methods used in this demo, see:"}
                    <ul>
                      <li>{"gather: "}<a href="https://dib-lab.github.io/2020-paper-sourmash-gather/">{"Lightweight compositional analysis of metagenomes with sourmash gather"}</a>{"."}</li>
                      <li>{"sourmash: "}<a href="https://doi.org/10.12688/f1000research.19675.1">{"Large-scale sequence comparisons with sourmash"}</a>{"."}</li>
                      <li>{"sourmash in the browser: "}<a href="https://blog.luizirber.org/2018/08/27/sourmash-wasm/">{"Oxidizing sourmash: WebAssembly"}</a>{"."}</li>
                      <li>{"Rust and WebAssembly: "}<a href="https://rustwasm.github.io/docs/book/">{"The Rust and WebAssembly book"}</a>{"."}</li>
                    </ul>
                </p>

                <p>
                  {"Additional thanks to the "}<a href="https://github.com/ipfs/js-ipfs/tree/master/examples/browser-exchange-files">
                  {"Exchange files between the browser and other IPFS nodes"}</a>{" example from "}
                  <a href="https://github.com/ipfs/js-ipfs">{"js-ipfs"}</a>{", "}
                  {"from where most of the UI/frontend was adapted."}
                </p>
              </div>
            </div>
          </>
        }
    }

    fn change(&mut self, _props: Self::Properties) -> ShouldRender {
        false
    }
}
