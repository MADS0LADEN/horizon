//! Client want-set for LSS column streaming.
use super::wire::{self, SessionConfig};
use crate::fast_hash::{FxHashMap, FxHashSet};

pub const WANT_SET_BUDGET: usize = 800;
pub const PRUNE_BUFFER: i32 = 32;
pub const FALLBACK_TICKS: u32 = 20;
pub const FAST_TICKS: u32 = 5;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub known: usize,
    pub received: u64,
    pub up_to_date: u64,
    pub not_generated: u64,
    pub in_flight: usize,
    pub confirmed_ring: i32,
    pub distance: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ColumnState {
    ts: i64,
    dirty: bool,
    settled: bool,
}

struct SessionInner {
    enabled: bool,
    server_lod: i32,
    client_distance: i32,
    center: (i32, i32),
    dimension: Option<String>,
    columns: FxHashMap<(i32, i32), ColumnState>,
    in_flight: FxHashSet<(i32, i32)>,
    confirmed_ring: i32,
    ticks_since_scan: u32,
    last_declared: usize,
    last_answered: usize,
    backpressure_clear_sent: bool,
    force_scan: bool,
    last_prune_center: (i32, i32),
    received: u64,
    up_to_date: u64,
    not_generated: u64,
}

pub struct Session {
    inner: SessionInner,
}

impl Session {
    pub fn new() -> Self {
        Self {
            inner: SessionInner {
                enabled: false,
                server_lod: 0,
                client_distance: 0,
                center: (0, 0),
                dimension: None,
                columns: FxHashMap::default(),
                in_flight: FxHashSet::default(),
                confirmed_ring: 0,
                ticks_since_scan: FALLBACK_TICKS,
                last_declared: 0,
                last_answered: 0,
                backpressure_clear_sent: false,
                force_scan: true,
                last_prune_center: (0, 0),
                received: 0,
                up_to_date: 0,
                not_generated: 0,
            },
        }
    }

    pub fn configure(&mut self, config: &SessionConfig) {
        self.inner.enabled =
            config.protocol == wire::PROTOCOL_VERSION && config.enabled && config.lod_distance > 0;
        self.inner.server_lod = config.lod_distance.max(0);
        self.inner.force_scan = true;
        self.inner.ticks_since_scan = FALLBACK_TICKS;
    }

    pub fn enabled(&self) -> bool {
        self.inner.enabled
    }

    pub fn enter_dimension(
        &mut self,
        dimension: &str,
        stored: impl IntoIterator<Item = ((i32, i32), i64)>,
    ) {
        self.inner.columns.clear();
        self.inner.in_flight.clear();
        self.inner.dimension = Some(dimension.to_owned());
        self.inner.confirmed_ring = 0;
        self.inner.last_declared = 0;
        self.inner.last_answered = 0;
        self.inner.force_scan = true;
        self.inner.ticks_since_scan = FALLBACK_TICKS;
        for (pos, ts) in stored {
            if ts > 0 {
                self.inner.columns.insert(
                    pos,
                    ColumnState {
                        ts,
                        dirty: false,
                        settled: false,
                    },
                );
            }
        }
    }

    pub fn dimension(&self) -> Option<&str> {
        self.inner.dimension.as_deref()
    }

    pub fn set_view(&mut self, center: (i32, i32), client_distance: i32) {
        let old = self.inner.center;
        self.inner.client_distance = client_distance.max(0);
        self.inner.center = center;
        let delta = chebyshev(old, center);
        if delta > 0 {
            self.inner.confirmed_ring = (self.inner.confirmed_ring - delta).max(0);
        }
        if chebyshev(self.inner.last_prune_center, center) >= 8 {
            self.prune();
            self.inner.last_prune_center = center;
        }
        if delta > 0 {
            self.inner.force_scan = true;
        }
    }

    pub fn distance(&self) -> i32 {
        effective_distance(self.inner.server_lod, self.inner.client_distance)
    }

    /// The distance the server offers in this level, before the client's cap.
    pub fn server_distance(&self) -> i32 {
        self.inner.server_lod
    }

    pub fn on_column(&mut self, dimension: &str, pos: (i32, i32), timestamp: i64) -> bool {
        if self.inner.dimension.as_deref() != Some(dimension) {
            return false;
        }
        if !in_range(pos, self.inner.center, self.distance() + PRUNE_BUFFER) {
            return false;
        }
        self.inner.columns.insert(
            pos,
            ColumnState {
                ts: timestamp,
                dirty: false,
                settled: true,
            },
        );
        self.inner.received += 1;
        self.mark_answered(pos);
        true
    }

    /// An answer from the server's batch. `level` is the level the session is
    /// streaming now; `lss:batch_response` carries no dimension, so an answer
    /// that does not belong to this level is recognised by what this level
    /// asked for — see the `in_flight` check below.
    ///
    /// One window stays open, and the wire cannot close it: an answer to a
    /// request the level left had in flight for a position the level entered
    /// also asks for carries no dimension and no request id, so it reads as
    /// this level's. It settles a column this level never fetched, which then
    /// reads as up to date and is not asked for again until a dirty notice or
    /// a new `enter_dimension`. The terrain it costs is that one column in the
    /// new level (each level has its own store, so no other level's bytes can
    /// be shown); closing it needs a protocol change.
    pub fn on_response(&mut self, level: &str, response: wire::Response, pos: (i32, i32)) {
        if self.inner.dimension.as_deref() != Some(level) {
            return;
        }
        if !in_range(pos, self.inner.center, self.distance()) {
            return;
        }
        // Only this level's own requests are answered here. `enter_dimension`
        // empties `in_flight` and `tick` refills it, so an answer to a request
        // the previous level sent is dropped instead of settling a column this
        // level never fetched (the two levels can share a dimension type and
        // so the same coordinates).
        if !self.inner.in_flight.contains(&pos) {
            return;
        }
        match response {
            wire::Response::UpToDate => {
                let entry = self.inner.columns.entry(pos).or_insert(ColumnState {
                    ts: -1,
                    dirty: false,
                    settled: false,
                });
                entry.dirty = false;
                entry.settled = true;
                self.inner.up_to_date += 1;
                self.mark_answered(pos);
            }
            wire::Response::NotGenerated => {
                let entry = self.inner.columns.entry(pos).or_insert(ColumnState {
                    ts: -1,
                    dirty: false,
                    settled: false,
                });
                entry.dirty = false;
                entry.settled = true;
                self.inner.not_generated += 1;
                self.mark_answered(pos);
            }
            wire::Response::Unknown(_) => {}
        }
    }

    /// A server dirty notice. `lss:dirty_columns` carries no dimension either,
    /// so a notice is matched against the level that asked: a column this
    /// level never fetched is not its terrain, whatever shares its coordinates.
    pub fn on_dirty(&mut self, level: &str, positions: &[(i32, i32)]) {
        if self.inner.dimension.as_deref() != Some(level) {
            return;
        }
        let distance = self.distance();
        for &pos in positions {
            if !in_range(pos, self.inner.center, distance) {
                continue;
            }
            if let Some(state) = self.inner.columns.get_mut(&pos) {
                state.dirty = true;
                state.settled = false;
                reopen_ring(&mut self.inner.confirmed_ring, pos, self.inner.center);
                self.inner.force_scan = true;
            }
        }
    }

    pub fn on_ingest_failed(&mut self, pos: (i32, i32)) {
        if let Some(state) = self.inner.columns.get_mut(&pos) {
            state.ts = -1;
            state.dirty = false;
            state.settled = false;
            reopen_ring(&mut self.inner.confirmed_ring, pos, self.inner.center);
        }
    }

    pub fn tick(&mut self, backlog: usize, halt: usize) -> Option<Vec<u8>> {
        if !self.inner.enabled || self.inner.dimension.is_none() {
            return None;
        }
        if backlog >= halt {
            if !self.inner.backpressure_clear_sent {
                self.inner.backpressure_clear_sent = true;
                return Some(wire::batch_request(&[]));
            }
            return None;
        }
        self.inner.backpressure_clear_sent = false;
        self.inner.ticks_since_scan += 1;
        let fast = self.inner.last_declared > 0
            && self.inner.last_answered * 20 >= self.inner.last_declared * 19;
        let due = self.inner.force_scan
            || self.inner.ticks_since_scan >= FALLBACK_TICKS
            || (self.inner.ticks_since_scan >= FAST_TICKS && fast);
        if !due {
            return None;
        }
        self.inner.force_scan = false;
        self.inner.ticks_since_scan = 0;
        let wants = self.scan();
        self.inner.last_declared = wants.len();
        self.inner.last_answered = 0;
        self.inner.in_flight.clear();
        for &(pos, _) in &wants {
            self.inner.in_flight.insert(pos);
        }
        if wants.is_empty() {
            None
        } else {
            Some(wire::batch_request(&wants))
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            known: self.inner.columns.len(),
            received: self.inner.received,
            up_to_date: self.inner.up_to_date,
            not_generated: self.inner.not_generated,
            in_flight: self.inner.in_flight.len(),
            confirmed_ring: self.inner.confirmed_ring,
            distance: self.distance(),
        }
    }

    fn prune(&mut self) {
        let limit = self.distance() + PRUNE_BUFFER;
        self.inner
            .columns
            .retain(|&pos, _| chebyshev(pos, self.inner.center) <= limit);
        self.inner
            .in_flight
            .retain(|&pos| chebyshev(pos, self.inner.center) <= limit);
    }

    fn mark_answered(&mut self, pos: (i32, i32)) {
        if self.inner.in_flight.contains(&pos) {
            self.inner.last_answered += 1;
        }
    }

    fn classify(&self, pos: (i32, i32)) -> Option<i64> {
        let Some(state) = self.inner.columns.get(&pos) else {
            return Some(-1);
        };
        if state.dirty {
            return Some(if state.ts > 0 { state.ts } else { -1 });
        }
        if state.settled {
            return None;
        }
        if state.ts > 0 {
            return Some(state.ts);
        }
        Some(-1)
    }

    fn scan(&mut self) -> Vec<((i32, i32), i64)> {
        let distance = self.distance();
        let center = self.inner.center;
        let mut out = Vec::new();
        let mut first_wanted_ring = distance + 1;
        for r in self.inner.confirmed_ring..=distance {
            if out.len() >= WANT_SET_BUDGET {
                break;
            }
            for pos in ring_positions(r, center) {
                let dx = pos.0 - center.0;
                let dz = pos.1 - center.1;
                if i64::from(dx) * i64::from(dx) + i64::from(dz) * i64::from(dz)
                    > i64::from(distance) * i64::from(distance)
                {
                    continue;
                }
                let Some(ts) = self.classify(pos) else {
                    continue;
                };
                if out.len() < WANT_SET_BUDGET {
                    first_wanted_ring = first_wanted_ring.min(r);
                    out.push((pos, ts));
                }
            }
        }
        self.inner.confirmed_ring = first_wanted_ring;
        out
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl Session {
    fn arm_scan_for_test(&mut self) {
        self.inner.ticks_since_scan = FALLBACK_TICKS;
        self.inner.force_scan = true;
    }
}

fn effective_distance(server: i32, client: i32) -> i32 {
    let mut d = server.max(1);
    if client > 0 {
        d = d.min(client).max(1);
    }
    d
}

fn chebyshev(a: (i32, i32), b: (i32, i32)) -> i32 {
    (a.0 - b.0).abs().max((a.1 - b.1).abs())
}

fn in_range(pos: (i32, i32), center: (i32, i32), distance: i32) -> bool {
    chebyshev(pos, center) <= distance
}

fn reopen_ring(confirmed: &mut i32, pos: (i32, i32), center: (i32, i32)) {
    *confirmed = (*confirmed).min(chebyshev(pos, center));
}

fn ring_positions(r: i32, center: (i32, i32)) -> Vec<(i32, i32)> {
    let (cx, cz) = center;
    if r == 0 {
        return vec![(cx, cz)];
    }
    let r_usize = r as usize;
    let mut out = Vec::with_capacity(8 * r_usize);
    for i in 0..8 * r_usize {
        let edge = i / (2 * r_usize);
        let pos = i % (2 * r_usize);
        out.push(match edge {
            0 => (cx - r + pos as i32, cz - r),
            1 => (cx + r, cz - r + pos as i32),
            2 => (cx + r - pos as i32, cz + r),
            _ => (cx - r, cz + r - pos as i32),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire::SessionConfig;

    fn decode_batch(body: &[u8]) -> Vec<((i32, i32), i64)> {
        let mut r = horizon_protocol::buf::Reader::new(body);
        let count = r.varint().unwrap() as usize;
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let packed = r.i64().unwrap();
            let ts = r.i64().unwrap();
            out.push((wire::unpack(packed), ts));
        }
        out
    }

    fn enabled_session(lod: i32) -> Session {
        let mut s = Session::new();
        s.configure(&SessionConfig {
            protocol: wire::PROTOCOL_VERSION,
            enabled: true,
            lod_distance: lod,
            generation: true,
            data_version: None,
            flags: None,
        });
        s.enter_dimension("minecraft:overworld", []);
        s.set_view((0, 0), 0);
        s
    }

    fn arm_scan(s: &mut Session) {
        s.arm_scan_for_test();
    }

    #[test]
    fn classification_ladder() {
        let mut s = enabled_session(8);
        s.on_column("minecraft:overworld", (1, 0), 5);
        s.on_dirty("minecraft:overworld", &[(1, 0)]);
        let body = s.tick(0, 9999).expect("dirty resend");
        assert!(decode_batch(&body).contains(&((1, 0), 5)));

        s.on_response("minecraft:overworld", wire::Response::UpToDate, (2, 0));
        arm_scan(&mut s);
        let body = s.tick(0, 9999);
        assert!(body.is_none() || !decode_batch(&body.unwrap()).iter().any(|w| w.0 == (2, 0)));

        s.enter_dimension("minecraft:overworld", [((3, 0), 9)]);
        s.set_view((0, 0), 0);
        let body = s.tick(0, 9999).expect("cached ts");
        assert!(decode_batch(&body).contains(&((3, 0), 9)));
    }

    #[test]
    fn walk_order_nearest_first() {
        let mut s = enabled_session(2);
        let body = s.tick(0, 9999).expect("scan");
        let wants = decode_batch(&body);
        assert_eq!(wants.first().map(|w| w.0), Some((0, 0)));
        assert!(
            wants
                .windows(2)
                .all(|w| { chebyshev(w[0].0, (0, 0)) <= chebyshev(w[1].0, (0, 0)) })
        );
    }

    #[test]
    fn budget_cap_800() {
        let mut s = enabled_session(512);
        let body = s.tick(0, 9999).expect("scan");
        assert!(decode_batch(&body).len() <= WANT_SET_BUDGET);
    }

    #[test]
    fn circle_skip_excludes_far_corners() {
        let mut s = enabled_session(2);
        let body = s.tick(0, 9999).expect("scan");
        let wants = decode_batch(&body);
        for (pos, _) in wants {
            let dx = pos.0;
            let dz = pos.1;
            assert!(i64::from(dx) * i64::from(dx) + i64::from(dz) * i64::from(dz) <= 8);
        }
    }

    #[test]
    fn confirmed_ring_moves_with_settlement() {
        let mut s = enabled_session(1);
        let _ = s.tick(0, 9999);
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (0, 0));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (1, 0));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (0, 1));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (1, 1));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (-1, 0));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (0, -1));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (-1, -1));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (1, -1));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (-1, 1));
        arm_scan(&mut s);
        let _ = s.tick(0, 9999);
        assert!(s.stats().confirmed_ring > 0);
    }

    #[test]
    fn confirmed_ring_drops_on_move() {
        let mut s = enabled_session(4);
        let _ = s.tick(0, 9999);
        let before = s.stats().confirmed_ring;
        s.set_view((3, 0), 0);
        assert!(s.stats().confirmed_ring <= before);
    }

    #[test]
    fn dirty_reopens_and_redeclares_with_ts() {
        let mut s = enabled_session(2);
        s.on_column("minecraft:overworld", (2, 0), 50);
        s.on_dirty("minecraft:overworld", &[(2, 0)]);
        let body = s.tick(0, 9999).expect("scan");
        assert!(decode_batch(&body).contains(&((2, 0), 50)));
    }

    #[test]
    fn not_generated_not_redeclared() {
        let mut s = enabled_session(2);
        s.on_response("minecraft:overworld", wire::Response::NotGenerated, (5, 0));
        arm_scan(&mut s);
        let body = s.tick(0, 9999);
        assert!(body.is_none() || !decode_batch(&body.unwrap()).contains(&((5, 0), -1)));
    }

    #[test]
    fn seeded_timestamp_up_to_date() {
        let mut s = Session::new();
        s.configure(&SessionConfig {
            protocol: wire::PROTOCOL_VERSION,
            enabled: true,
            lod_distance: 2,
            generation: true,
            data_version: None,
            flags: None,
        });
        s.enter_dimension("minecraft:overworld", [((1, 0), 99)]);
        s.set_view((0, 0), 0);
        let body = s.tick(0, 9999).expect("scan");
        assert!(decode_batch(&body).contains(&((1, 0), 99)));
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (1, 0));
        arm_scan(&mut s);
        let body = s.tick(0, 9999);
        assert!(body.is_none() || !decode_batch(&body.unwrap()).iter().any(|w| w.0 == (1, 0)));
    }

    #[test]
    fn backpressure_empty_batch_once() {
        let mut s = enabled_session(2);
        let first = s.tick(10, 5);
        assert_eq!(first.as_deref(), Some(wire::batch_request(&[]).as_slice()));
        assert!(s.tick(10, 5).is_none());
        assert!(s.tick(0, 5).is_some());
    }

    #[test]
    fn fallback_and_fast_cadence() {
        let mut s = enabled_session(1);
        let _ = s.tick(0, 9999);
        for _ in 0..FALLBACK_TICKS - 1 {
            assert!(
                s.tick(0, 9999).is_none(),
                "scan should not run before fallback"
            );
        }
        assert!(s.tick(0, 9999).is_some(), "fallback scan due");
    }

    #[test]
    fn disabled_sends_nothing() {
        let mut s = Session::new();
        s.configure(&SessionConfig {
            protocol: wire::PROTOCOL_VERSION,
            enabled: false,
            lod_distance: 8,
            generation: true,
            data_version: None,
            flags: None,
        });
        s.enter_dimension("minecraft:overworld", []);
        assert!(s.tick(0, 9999).is_none());
    }

    #[test]
    fn other_dimension_column_rejected() {
        let mut s = enabled_session(2);
        assert!(!s.on_column("minecraft:the_nether", (0, 0), 1));
    }

    /// Two levels of one dimension type: `minecraft:overworld` and a datapack
    /// level `hoxy:test2` that is declared `minecraft:overworld` too. The
    /// level key separates their sessions, so an answer or a dirty notice that
    /// belongs to the level just left is not applied to the one entered.
    #[test]
    fn same_type_levels_do_not_share_answers() {
        let mut s = enabled_session(2);
        let _ = s.tick(0, 9999);
        // The overworld's request for (1, 0) is answered by the server...
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (1, 0));
        assert_eq!(s.stats().up_to_date, 1);

        // ...then the player enters the other overworld-type level at the same
        // centre, so the same coordinates are wanted there. The server's answer
        // to the overworld's request must not settle this level's column.
        s.enter_dimension("hoxy:test2", []);
        s.set_view((0, 0), 0);
        let before = s.stats().up_to_date;
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (1, 0));
        assert_eq!(
            s.stats().up_to_date,
            before,
            "the level left answered for the level entered"
        );
        // The same level's own answer is taken.
        let _ = s.tick(0, 9999);
        s.on_response("hoxy:test2", wire::Response::UpToDate, (1, 0));
        assert_eq!(s.stats().up_to_date, before + 1);
    }

    /// A column fetched in one level never becomes the other level's: entering
    /// the second level starts its want-set from that level's own store.
    #[test]
    fn same_type_levels_do_not_share_columns() {
        let mut s = enabled_session(2);
        s.on_column("minecraft:overworld", (1, 0), 5);
        assert_eq!(s.stats().known, 1);

        s.enter_dimension("hoxy:test2", []);
        s.set_view((0, 0), 0);
        assert_eq!(s.stats().known, 0, "the other level's column stayed");
        // Its store has its own stamps, and they are what it starts from.
        s.enter_dimension("hoxy:test2", [((1, 0), 7)]);
        s.set_view((0, 0), 0);
        let body = s.tick(0, 9999).expect("scan");
        assert!(decode_batch(&body).contains(&((1, 0), 7)));
    }

    /// A dirty notice carries no dimension. The level streamed now only
    /// reopens columns it holds, so the level left's notice cannot re-request
    /// this level's terrain.
    #[test]
    fn same_type_levels_do_not_share_dirty_notices() {
        let mut s = enabled_session(2);
        let _ = s.tick(0, 9999);
        s.on_response("minecraft:overworld", wire::Response::UpToDate, (0, 0));
        assert_eq!(s.stats().up_to_date, 1);

        // The other overworld-type level, settled at the same coordinates.
        s.enter_dimension("hoxy:test2", []);
        s.set_view((0, 0), 0);
        let _ = s.tick(0, 9999);
        s.on_response("hoxy:test2", wire::Response::UpToDate, (0, 0));
        assert_eq!(s.stats().up_to_date, 2);

        // The overworld's notice for the same coordinates is not this level's.
        s.on_dirty("minecraft:overworld", &[(0, 0)]);
        arm_scan(&mut s);
        let body = s.tick(0, 9999);
        assert!(
            body.is_none() || !decode_batch(&body.unwrap()).iter().any(|w| w.0 == (0, 0)),
            "the level left's dirty notice re-requested the level entered's column"
        );
    }

    #[test]
    fn prune_after_eight_chunk_move() {
        let mut s = enabled_session(4);
        s.enter_dimension("minecraft:overworld", [((100, 0), 1)]);
        s.set_view((0, 0), 0);
        s.set_view((8, 0), 0);
        assert_eq!(s.stats().known, 0);
    }

    #[test]
    fn ingest_failed_reopens() {
        let mut s = enabled_session(2);
        s.on_column("minecraft:overworld", (1, 0), 10);
        s.on_ingest_failed((1, 0));
        assert!(s.stats().confirmed_ring <= chebyshev((1, 0), (0, 0)));
    }
}
