//! Background decode of LSS voxel columns into stored far-terrain columns.
use super::store::Store;
use super::wire;
use crate::biome::Biomes;
use crate::chunk::ColumnLayout;
use crate::lod::{self, Column, ReducedColumns};
use horizon_world::registry::BlockRegistry;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

pub struct Context {
    pub registry: &'static BlockRegistry,
    pub biomes: Arc<Biomes>,
    pub kinds: Arc<Vec<u8>>,
    pub layout: ColumnLayout,
    pub sky_light: bool,
    pub store: Arc<Store>,
}

pub struct Ingested {
    pub pos: (i32, i32),
    pub result: Result<(ReducedColumns, Column), String>,
}

struct Job {
    column: wire::VoxelColumn,
}

pub struct Ingest {
    submit_tx: Option<SyncSender<Job>>,
    done_rx: Receiver<Ingested>,
    backlog: Arc<AtomicUsize>,
    workers: Vec<JoinHandle<()>>,
}

impl Ingest {
    pub const CAPACITY: usize = 1024;

    pub fn new(context: Context, threads: usize) -> Self {
        let (submit_tx, submit_rx) = mpsc::sync_channel::<Job>(Self::CAPACITY);
        let submit_rx = Arc::new(Mutex::new(submit_rx));
        let (done_tx, done_rx) = mpsc::channel::<Ingested>();
        let backlog = Arc::new(AtomicUsize::new(0));
        let thread_count = threads.max(1);
        let mut workers = Vec::with_capacity(thread_count);
        for id in 0..thread_count {
            let submit_rx = submit_rx.clone();
            let done_tx = done_tx.clone();
            let backlog = backlog.clone();
            let context = Context {
                registry: context.registry,
                biomes: context.biomes.clone(),
                kinds: context.kinds.clone(),
                layout: context.layout,
                sky_light: context.sky_light,
                store: context.store.clone(),
            };
            workers.push(
                thread::Builder::new()
                    .name(format!("LSS decode {id}"))
                    .spawn(move || worker_loop(submit_rx, done_tx, backlog, context))
                    .expect("LSS decode thread"),
            );
        }
        Self {
            submit_tx: Some(submit_tx),
            done_rx,
            backlog,
            workers,
        }
    }

    pub fn submit(&self, column: wire::VoxelColumn) -> bool {
        let Some(tx) = &self.submit_tx else {
            return false;
        };
        if self.backlog.load(Ordering::Relaxed) >= Self::CAPACITY {
            return false;
        }
        self.backlog.fetch_add(1, Ordering::Relaxed);
        if tx.send(Job { column }).is_err() {
            self.backlog.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    pub fn backlog(&self) -> usize {
        self.backlog.load(Ordering::Relaxed)
    }

    pub fn drain(&self) -> Vec<Ingested> {
        let mut out = Vec::new();
        while let Ok(item) = self.done_rx.try_recv() {
            out.push(item);
        }
        out
    }
}

impl Drop for Ingest {
    fn drop(&mut self) {
        self.submit_tx.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn worker_loop(
    submit_rx: Arc<Mutex<Receiver<Job>>>,
    done_tx: Sender<Ingested>,
    backlog: Arc<AtomicUsize>,
    context: Context,
) {
    let resolver = RefCell::new(super::section::Resolver::new(
        context.registry,
        context.biomes.clone(),
    ));
    loop {
        let job = {
            let rx = submit_rx.lock().unwrap_or_else(|e| e.into_inner());
            match rx.recv() {
                Ok(job) => job,
                Err(_) => break,
            }
        };
        let pos = job.column.pos;
        let result = decode_column(job.column, &context, &resolver);
        backlog.fetch_sub(1, Ordering::Relaxed);
        let _ = done_tx.send(Ingested { pos, result });
    }
}

fn decode_column(
    column: wire::VoxelColumn,
    context: &Context,
    resolver: &RefCell<super::section::Resolver>,
) -> Result<(ReducedColumns, Column), String> {
    let bytes = wire::section_bytes(&column)?;
    let chunk = super::section::decode(
        &bytes,
        column.pos,
        context.layout,
        context.sky_light,
        &mut |s| resolver.borrow_mut().block(s),
        &mut |s| resolver.borrow_mut().biome(s),
    )?;
    let columns = lod::summarize(&chunk, &context.kinds);
    let coarse = lod::coarse(&columns);
    context.store.put(
        column.pos,
        column.timestamp,
        columns.as_ref(),
        coarse.clone(),
    );
    // Every width `Lod::apply_columns` reads, built here on the decode thread
    // rather than on the frame that applies them.
    Ok((ReducedColumns::new(columns).warm(0b1111), coarse))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir() -> std::path::PathBuf {
        std::env::temp_dir().join(format!("horizon-lss-ingest-{}", std::process::id()))
    }

    #[test]
    fn ingest_decodes_fixture_column() {
        let root = std::env::var_os("HORIZON_TEST_RESOURCES");
        if root.is_none() {
            eprintln!("skip ingest_decodes_fixture_column: no HORIZON_TEST_RESOURCES");
            return;
        }
        let root = std::path::PathBuf::from(root.unwrap());
        let assets = crate::assets::Assets::load(&root).expect("assets");
        let biomes = Arc::new(crate::biome::Biomes::load(&root).expect("biomes"));
        let facts = crate::blocks::BlockFacts::load(&root, assets.registry).expect("facts");
        let kinds = Arc::new(lod::state_kinds(&facts));
        let dir = temp_dir();
        let _ = std::fs::remove_dir_all(&dir);
        let store = Arc::new(
            Store::open(
                dir.clone(),
                Store::key(facts.states.len(), biomes.list.len()),
            )
            .unwrap(),
        );
        let ingest = Ingest::new(
            Context {
                registry: assets.registry,
                biomes,
                kinds,
                layout: ColumnLayout::default(),
                sky_light: true,
                store: store.clone(),
            },
            2,
        );
        let column = wire::VoxelColumn {
            pos: (0, 0),
            dimension: "minecraft:overworld".into(),
            timestamp: 42,
            source: 0,
            codec: wire::CODEC_RAW,
            sections: include_bytes!("testdata/col-00.bin").to_vec(),
        };
        assert!(ingest.submit(column));
        for _ in 0..100 {
            let done = ingest.drain();
            if !done.is_empty() {
                assert!(done[0].result.is_ok());
                assert!(store.columns((0, 0)).is_some());
                let _ = std::fs::remove_dir_all(&dir);
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("ingest did not finish");
    }
}
