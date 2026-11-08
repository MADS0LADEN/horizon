//! Game-facing LSS client: session, storage, decode, and far-terrain source.
use super::ingest::{Context, Ingest};
use super::session::Session;
use super::store::Store;
use super::wire;
use crate::biome::Biomes;
use crate::chunk::ColumnLayout;
use crate::lod::{Column, ReducedColumns};
use crate::streaming::LodSource;
use horizon_world::registry::BlockRegistry;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Shared {
    pub registry: &'static BlockRegistry,
    pub biomes: Arc<Biomes>,
    pub kinds: Arc<Vec<u8>>,
}

pub struct Dimension {
    pub name: String,
    pub layout: ColumnLayout,
    pub sky_light: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub session: super::session::Stats,
    pub backlog: usize,
    pub stored: usize,
}

struct Level {
    dimension: Dimension,
    store: Arc<Store>,
    ingest: Ingest,
}

pub struct Client {
    root: PathBuf,
    shared: Shared,
    session: Session,
    level: Option<Level>,
    storage_key: u64,
}

impl Client {
    pub const HALT: usize = Ingest::CAPACITY * 3 / 4;

    pub fn new(root: PathBuf, shared: Shared) -> Self {
        let storage_key = Store::key(shared.registry.len(), shared.biomes.list.len());
        Self {
            root,
            shared,
            session: Session::new(),
            level: None,
            storage_key,
        }
    }

    pub fn hello(&self, data_version: i32) -> Vec<(String, Vec<u8>)> {
        vec![
            ("minecraft:register".into(), wire::register()),
            (
                wire::HANDSHAKE.into(),
                wire::handshake(
                    wire::CAP_VOXEL_COLUMNS | wire::CAP_ZSTD_COLUMNS | wire::CAP_MAP_POLICY,
                ),
            ),
            (wire::CLIENT_INFO.into(), wire::client_info(data_version)),
        ]
    }

    pub fn enter_dimension(&mut self, dimension: Dimension) {
        self.level = None;
        let dir = level_dir(&self.root, &dimension.name);
        match Store::open(dir, self.storage_key) {
            Ok(store) => {
                let store = Arc::new(store);
                let stamps = store.stamps();
                self.session.enter_dimension(&dimension.name, stamps);
                let ingest = Ingest::new(
                    Context {
                        registry: self.shared.registry,
                        biomes: self.shared.biomes.clone(),
                        kinds: self.shared.kinds.clone(),
                        layout: dimension.layout,
                        sky_light: dimension.sky_light,
                        store: store.clone(),
                    },
                    2,
                );
                self.level = Some(Level {
                    dimension,
                    store,
                    ingest,
                });
            }
            Err(error) => {
                log::warn!("LSS store for {} failed to open: {error}", dimension.name);
                self.session.enter_dimension(&dimension.name, []);
            }
        }
    }

    pub fn on_payload(&mut self, channel: &str, data: &[u8]) -> bool {
        if !channel.starts_with("lss:") {
            return false;
        }
        match channel {
            wire::SESSION_CONFIG => match wire::session_config(data) {
                Ok(config) => {
                    self.session.configure(&config);
                }
                Err(error) => log::debug!("LSS session_config: {error}"),
            },
            wire::BATCH_RESPONSE => match wire::batch_response(data) {
                Ok(responses) => {
                    // `lss:batch_response` carries no dimension: it answers the
                    // requests this level sent, and the session drops any other
                    // (`Session::on_response`).
                    let Some(level) = self.level().map(str::to_owned) else {
                        return true;
                    };
                    for (response, pos) in responses {
                        self.session.on_response(&level, response, pos);
                    }
                }
                Err(error) => log::debug!("LSS batch_response: {error}"),
            },
            wire::VOXEL_COLUMN => {
                let Ok(column) = wire::voxel_column(data) else {
                    log::debug!("LSS voxel_column: malformed");
                    return true;
                };
                let Some(level) = self.level.as_mut() else {
                    return true;
                };
                if column.dimension != level.dimension.name {
                    return true;
                }
                if !self
                    .session
                    .on_column(&level.dimension.name, column.pos, column.timestamp)
                {
                    return true;
                }
                let column_pos = column.pos;
                if !level.ingest.submit(column) {
                    self.session.on_ingest_failed(column_pos);
                }
            }
            wire::DIRTY_COLUMNS => match wire::dirty_columns(data) {
                Ok(positions) => {
                    // No dimension in the payload: `Session::on_dirty` keeps
                    // only positions this level has (another level's notice is
                    // not this level's terrain).
                    let Some(level) = self.level().map(str::to_owned) else {
                        return true;
                    };
                    self.session.on_dirty(&level, &positions);
                }
                Err(error) => log::debug!("LSS dirty_columns: {error}"),
            },
            _ => log::debug!("LSS unknown channel {channel}"),
        }
        true
    }

    pub fn tick(&mut self, center: (i32, i32), client_distance: i32) -> Option<Vec<u8>> {
        let backlog = self
            .level
            .as_ref()
            .map(|level| level.ingest.backlog())
            .unwrap_or(0);
        self.session.set_view(center, client_distance);
        self.session.tick(backlog, Self::HALT)
    }

    pub fn drain(&mut self) -> Vec<((i32, i32), ReducedColumns, Column)> {
        let Some(level) = self.level.as_mut() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for item in level.ingest.drain() {
            match item.result {
                Ok((reduced, coarse)) => out.push((item.pos, reduced, coarse)),
                Err(_) => self.session.on_ingest_failed(item.pos),
            }
        }
        out
    }

    pub fn source(&self) -> Option<Arc<dyn LodSource>> {
        if !self.active() {
            return None;
        }
        Some(self.level.as_ref().unwrap().store.clone())
    }

    pub fn active(&self) -> bool {
        self.session.enabled() && self.level.is_some()
    }

    /// The level key the session streams now (`hoxy:test2`, `minecraft:overworld`),
    /// as the server names it — the LSS key, not the dimension type two
    /// overworld-type levels would share.
    pub fn level(&self) -> Option<&str> {
        self.level
            .as_ref()
            .map(|level| level.dimension.name.as_str())
    }

    pub fn distance(&self) -> i32 {
        self.session.distance()
    }

    pub fn server_distance(&self) -> i32 {
        self.session.server_distance()
    }

    pub fn stats(&self) -> Stats {
        Stats {
            session: self.session.stats(),
            backlog: self
                .level
                .as_ref()
                .map(|level| level.ingest.backlog())
                .unwrap_or(0),
            stored: self
                .level
                .as_ref()
                .map(|level| level.store.len())
                .unwrap_or(0),
        }
    }
}

fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if matches!(c, 'a'..='z' | '0'..='9' | '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// A level's folder under the server's `lss/` folder. The level key is the
/// namespace and the path, so two levels of one dimension type — a datapack
/// `hoxy:test2` declared `minecraft:overworld`, next to the overworld — get
/// one folder each, and `hoxy:test2` cannot collide with `hoxy_test2` or with
/// the overworld's own `minecraft_overworld`.
fn level_dir(root: &Path, level: &str) -> PathBuf {
    root.join(sanitize_name(level))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::{self, CAP_MAP_POLICY, CAP_VOXEL_COLUMNS, CAP_ZSTD_COLUMNS};

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("horizon-lss-client-{}", std::process::id()))
    }

    /// A directory of its own: the other tests in this module delete
    /// `temp_root()`, and a test that shares it loses its files mid-run.
    fn temp_dir(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("horizon-lss-client-{tag}-{}", std::process::id()))
    }

    #[test]
    fn sanitize_dimension_folder_names() {
        assert_eq!(sanitize_name("minecraft:overworld"), "minecraft_overworld");
        assert_eq!(sanitize_name("still-fine.v2"), "still-fine.v2");
        assert_eq!(sanitize_name("Bad"), "_ad");
    }

    /// Two levels of one dimension type keep one folder each: the client keys
    /// LSS by the level key, so a datapack `hoxy:test2` declared
    /// `minecraft:overworld` does not share the overworld's store, and neither
    /// does an `event` level under it.
    #[test]
    fn same_type_levels_get_separate_store_dirs() {
        let root = PathBuf::from("/lss/mc.example");
        let overworld = level_dir(&root, "minecraft:overworld");
        let test2 = level_dir(&root, "hoxy:test2");
        let event = level_dir(&root, "minecraft:event");
        assert_eq!(overworld, root.join("minecraft_overworld"));
        assert_eq!(test2, root.join("hoxy_test2"));
        assert_eq!(event, root.join("minecraft_event"));
        assert_ne!(
            overworld, test2,
            "two levels sharing a dimension type shared a store"
        );
        assert_ne!(event, test2);
    }

    /// Their columns are independent: columns written to one store are not
    /// readable through the other, and the counts stay separate.
    #[test]
    fn same_type_levels_keep_independent_columns() {
        use crate::lod::{Span, Surface};
        use horizon_world::registry::BlockStateId;
        let root = temp_dir("columns");
        let _ = std::fs::remove_dir_all(&root);
        let overworld = Store::open(level_dir(&root, "minecraft:overworld"), 11).unwrap();
        let test2 = Store::open(level_dir(&root, "hoxy:test2"), 11).unwrap();
        let columns: [Column; 256] = std::array::from_fn(|i| Column {
            surface: Some(Surface {
                height: 64 + (i % 3) as i32,
                state: BlockStateId(i as u32),
                water: None,
                biome: 0,
            }),
            spans: vec![Span {
                min: 0,
                max: 1,
                state: BlockStateId(i as u32),
                biome: 0,
                light: 240,
            }]
            .into(),
        });
        let coarse = crate::lod::coarse(&columns);
        overworld.put((1, 0), 5, &columns, coarse.clone());
        assert_eq!(overworld.len(), 1);
        assert_eq!(
            test2.len(),
            0,
            "one level's column was in the other's store"
        );
        assert!(test2.stamps().is_empty());
        test2.put((2, 0), 9, &columns, coarse);
        assert_eq!(overworld.len(), 1);
        assert_eq!(test2.len(), 1);
        assert_eq!(overworld.stamps(), vec![((1, 0), 5)]);
        assert_eq!(test2.stamps(), vec![((2, 0), 9)]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn hello_packets_in_order() {
        let root = temp_root();
        let _ = std::fs::remove_dir_all(&root);
        let Some(resources) = std::env::var_os("HORIZON_TEST_RESOURCES") else {
            eprintln!("skip hello_packets_in_order: no HORIZON_TEST_RESOURCES");
            return;
        };
        let root_path = std::path::PathBuf::from(resources);
        let assets = crate::assets::Assets::load(&root_path).expect("assets");
        let biomes = Arc::new(crate::biome::Biomes::load(&root_path).expect("biomes"));
        let facts = crate::blocks::BlockFacts::load(&root_path, assets.registry).expect("facts");
        let shared = Shared {
            registry: assets.registry,
            biomes,
            kinds: Arc::new(crate::lod::state_kinds(&facts)),
        };
        let client = Client::new(root.clone(), shared);
        let hello = client.hello(777);
        assert_eq!(hello.len(), 3);
        assert_eq!(hello[0].0, "minecraft:register");
        assert_eq!(hello[1].0, wire::HANDSHAKE);
        assert_eq!(
            hello[1].1,
            wire::handshake(CAP_VOXEL_COLUMNS | CAP_ZSTD_COLUMNS | CAP_MAP_POLICY)
        );
        assert_eq!(hello[2].0, wire::CLIENT_INFO);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn session_config_enables_tick_batch() {
        let root = temp_root();
        let _ = std::fs::remove_dir_all(&root);
        let Some(resources) = std::env::var_os("HORIZON_TEST_RESOURCES") else {
            eprintln!("skip session_config_enables_tick_batch: no HORIZON_TEST_RESOURCES");
            return;
        };
        let root_path = std::path::PathBuf::from(resources);
        let assets = crate::assets::Assets::load(&root_path).expect("assets");
        let biomes = Arc::new(crate::biome::Biomes::load(&root_path).expect("biomes"));
        let facts = crate::blocks::BlockFacts::load(&root_path, assets.registry).expect("facts");
        let shared = Shared {
            registry: assets.registry,
            biomes,
            kinds: Arc::new(crate::lod::state_kinds(&facts)),
        };
        let mut client = Client::new(root.clone(), shared);
        client.enter_dimension(Dimension {
            name: "minecraft:overworld".into(),
            layout: ColumnLayout::default(),
            sky_light: true,
        });
        assert!(client.on_payload(wire::SESSION_CONFIG, &{
            let mut w = horizon_protocol::buf::Writer::new();
            w.varint(wire::PROTOCOL_VERSION);
            w.bool(true);
            w.varint(8);
            w.bool(true);
            w.data
        },));
        let mut body = None;
        for _ in 0..=crate::lss::session::FALLBACK_TICKS {
            body = client.tick((0, 0), 0);
            if body.is_some() {
                break;
            }
        }
        let body = body.expect("batch");
        assert!(!body.is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }
}
