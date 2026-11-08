//! Background decode of LSS voxel columns into stored far-terrain columns.
use super::store::Store;
use super::wire;
use crate::biome::Biomes;
use crate::chunk::ColumnLayout;
use crate::lod::{self, Column, ReducedColumns};
use horizon_world::registry::BlockRegistry;
use std::cell::RefCell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
    cancel: Arc<AtomicBool>,
    /// One `()` per worker as it returns, so `drop` can wait for them.
    stopped_rx: Receiver<()>,
    workers: Vec<JoinHandle<()>>,
}

/// How long a dropped session waits for its decode workers to finish the one
/// column each is on. They take no new job, so this covers a single decode and
/// its store write; a backlog of a thousand columns is never drained here.
const STOP_WAIT: std::time::Duration = std::time::Duration::from_millis(250);

/// The jobs a session's decode workers take, and the flag that ends them.
struct Queue {
    rx: Arc<Mutex<Receiver<Job>>>,
    cancel: Arc<AtomicBool>,
}

impl Queue {
    /// The next job, or `None` once the session is over. A session that ended
    /// stops at once: what is still queued is dropped, not decoded.
    fn next(&self) -> Option<Job> {
        if self.cancel.load(Ordering::Relaxed) {
            return None;
        }
        let rx = self.rx.lock().unwrap_or_else(|e| e.into_inner());
        rx.recv().ok()
    }
}

impl Ingest {
    pub const CAPACITY: usize = 1024;

    pub fn new(context: Context, threads: usize) -> Self {
        let (submit_tx, submit_rx) = mpsc::sync_channel::<Job>(Self::CAPACITY);
        let submit_rx = Arc::new(Mutex::new(submit_rx));
        let (done_tx, done_rx) = mpsc::channel::<Ingested>();
        let (stopped_tx, stopped_rx) = mpsc::channel::<()>();
        let backlog = Arc::new(AtomicUsize::new(0));
        let cancel = Arc::new(AtomicBool::new(false));
        let thread_count = threads.max(1);
        let mut workers = Vec::with_capacity(thread_count);
        for id in 0..thread_count {
            let queue = Queue {
                rx: submit_rx.clone(),
                cancel: cancel.clone(),
            };
            let done_tx = done_tx.clone();
            let stopped_tx = stopped_tx.clone();
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
                    .spawn(move || {
                        worker_loop(queue, done_tx, backlog, context);
                        // `drop` may already be gone; the send simply fails.
                        let _ = stopped_tx.send(());
                    })
                    .expect("LSS decode thread"),
            );
        }
        drop(stopped_tx);
        Self {
            submit_tx: Some(submit_tx),
            done_rx,
            backlog,
            cancel,
            stopped_rx,
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
        // The game thread drops the session: a disconnect, or another
        // dimension. It must not wait for the queue to drain -- a full backlog
        // is a thousand columns, seconds of decoding -- so the workers stop
        // taking jobs at once and finish only the column each is on. That is
        // all a reopen needs: the store handle closed, not the queue empty.
        self.cancel.store(true, Ordering::Relaxed);
        self.submit_tx.take();
        let queued = self.backlog.load(Ordering::Relaxed);
        if queued > 0 {
            log::info!(
                "LSS: {queued} columns were still queued when the session ended; \
                 the server sends them again next time"
            );
        }
        // A worker past `STOP_WAIT` is detached, not killed: a store the next
        // join reopens cuts a record it cut short off (`Store::open`), which is
        // what a kill -9 mid-stream leaves behind too.
        let deadline = std::time::Instant::now() + STOP_WAIT;
        let mut waiting = self.workers.len();
        while waiting > 0 {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            match self.stopped_rx.recv_timeout(left) {
                Ok(()) => waiting -= 1,
                Err(_) => break,
            }
        }
        // Joining a handle whose worker has returned is instant; one still
        // running is detached by the drop of its handle.
        self.workers.clear();
    }
}

fn worker_loop(
    queue: Queue,
    done_tx: Sender<Ingested>,
    backlog: Arc<AtomicUsize>,
    context: Context,
) {
    let resolver = RefCell::new(super::section::Resolver::new(
        context.registry,
        context.biomes.clone(),
    ));
    while let Some(job) = queue.next() {
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

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("horizon-lss-ingest-{tag}-{}", std::process::id()))
    }

    fn fixture_column(x: i32) -> wire::VoxelColumn {
        wire::VoxelColumn {
            pos: (x, 0),
            dimension: "minecraft:overworld".into(),
            timestamp: 42,
            source: 0,
            codec: wire::CODEC_RAW,
            sections: include_bytes!("testdata/col-00.bin").to_vec(),
        }
    }

    /// A session that ended leaves its queue undecoded: the game thread drops
    /// it on a disconnect, and draining a thousand columns there froze the
    /// window for seconds.
    #[test]
    fn a_cancelled_session_stops_instead_of_draining_its_queue() {
        let (tx, rx) = mpsc::sync_channel::<Job>(Ingest::CAPACITY);
        let cancel = Arc::new(AtomicBool::new(false));
        let queue = Queue {
            rx: Arc::new(Mutex::new(rx)),
            cancel: cancel.clone(),
        };
        for x in 0..8 {
            tx.send(Job {
                column: fixture_column(x),
            })
            .expect("queue");
        }
        cancel.store(true, Ordering::Relaxed);
        assert!(
            queue.next().is_none(),
            "a session that ended must not take another job"
        );
        // The jobs are still there: the stop is the flag, not an empty queue.
        cancel.store(false, Ordering::Relaxed);
        assert!(queue.next().is_some());
    }

    /// Dropping a session is what a disconnect does on the game thread: it
    /// must not decode the columns still queued. A full backlog (a thousand
    /// columns, seconds of decoding) used to be drained by `join` there, and
    /// the window stopped answering.
    #[test]
    fn dropping_a_session_leaves_the_backlog_undecoded() {
        let Some(root) = std::env::var_os("HORIZON_TEST_RESOURCES") else {
            eprintln!(
                "skip dropping_a_session_leaves_the_backlog_undecoded: no HORIZON_TEST_RESOURCES"
            );
            return;
        };
        let root = std::path::PathBuf::from(root);
        let assets = crate::assets::Assets::load(&root).expect("assets");
        let biomes = Arc::new(crate::biome::Biomes::load(&root).expect("biomes"));
        let facts = crate::blocks::BlockFacts::load(&root, assets.registry).expect("facts");
        let kinds = Arc::new(lod::state_kinds(&facts));
        let dir = temp_dir("drop");
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
        // A whole queue's worth of distinct columns, so every one the workers
        // did not reach is a column the store never gained.
        let mut queued = 0;
        for x in 0..500 {
            if ingest.submit(fixture_column(x)) {
                queued += 1;
            }
        }
        assert!(queued > 100, "only {queued} columns were queued");
        let before = store.len();
        let started = std::time::Instant::now();
        drop(ingest);
        let took = started.elapsed();
        assert!(
            took < std::time::Duration::from_millis(500),
            "dropping the session waited {took:?} for {queued} queued columns"
        );
        // What the workers had not reached when the session ended stays out of
        // the store: the queue is abandoned, not drained. (The two in flight
        // may land, so the bound is what a drain would have added.)
        let stored = store.len() - before;
        assert!(
            stored < queued / 2,
            "{stored} of {queued} queued columns were decoded after the session ended"
        );
        let _ = std::fs::remove_dir_all(&dir);
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
        let dir = temp_dir("fixture");
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
        let column = fixture_column(0);
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
