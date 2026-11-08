//! Far terrain from Java servers that run LOD Server Support / Voxy Server
//! Side (`horizon_client::lss`): the client half, in place of Voxy.
use super::Game;
use horizon_client::lss::client::{Client, Dimension, Shared};
use horizon_protocol::packets::{configuration::CustomPayload, play::Serverbound as S};
use std::sync::Arc;

/// A server's folder name under `lss/` in the game folder.
fn folder(address: &str) -> String {
    address
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '.' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

impl Game<'_> {
    /// handleLogin on a server: announce the client, which a server running
    /// LSS answers with its session. Single player keeps its own far terrain.
    pub(super) fn lss_join(&mut self) {
        if self.integrated.is_some() {
            return;
        }
        let Some(address) = self.server_address().map(folder) else {
            return;
        };
        let shared = Shared {
            registry: self.pack.assets.registry,
            biomes: self.res.biomes().clone(),
            kinds: Arc::new(horizon_client::lod::state_kinds(self.res.facts())),
        };
        let root = horizon_client::logging::game_home()
            .join("lss")
            .join(address);
        let client = Client::new(root, shared);
        let hello = client.hello(horizon_world::DATA_VERSION);
        let Some(r) = self.remote.as_mut() else {
            return;
        };
        r.lss = Some(client);
        for (channel, data) in hello {
            self.send(S::CustomPayload(CustomPayload { channel, data }));
        }
    }

    /// A new level: its columns are another store's.
    pub(super) fn lss_enter(&mut self, dimension: Dimension) {
        if let Some(client) = self.remote.as_mut().and_then(|r| r.lss.as_mut()) {
            client.enter_dimension(dimension);
        }
    }

    pub(super) fn lss_payload(&mut self, channel: &str, data: &[u8]) {
        let (changed, active, distance) = {
            let Some(client) = self.remote.as_mut().and_then(|r| r.lss.as_mut()) else {
                return;
            };
            let was_active = client.active();
            client.on_payload(channel, data);
            (
                client.active() != was_active,
                client.active(),
                client.server_distance(),
            )
        };
        if changed {
            log::info!(
                "LSS: far terrain from the server {}, {distance} chunks",
                if active { "on" } else { "off" }
            );
            // The source changes: far terrain is built again from the new one
            // (`prepare_lod`), rather than keeping the one it had.
            self.clear_lod();
        }
    }

    /// The want-set, once a tick.
    pub(super) fn tick_lss(&mut self) {
        let Some(center) = self.player.as_ref().map(|p| {
            (
                (p.position.x.floor() as i32) >> 4,
                (p.position.z.floor() as i32) >> 4,
            )
        }) else {
            return;
        };
        let distance = self.lod_distance() as i32;
        let Some(client) = self.remote.as_mut().and_then(|r| r.lss.as_mut()) else {
            return;
        };
        if let Some(data) = client.tick(center, distance) {
            self.send(S::CustomPayload(CustomPayload {
                channel: horizon_client::lss::wire::BATCH_REQUEST.into(),
                data,
            }));
        }
    }

    /// Decoded columns into the far terrain, once a frame.
    pub(super) fn lss_apply(&mut self) {
        let Some(client) = self.remote.as_mut().and_then(|r| r.lss.as_mut()) else {
            return;
        };
        let columns = client.drain();
        if let Some(lod) = self.lod.as_mut() {
            for (pos, reduced, coarse) in &columns {
                lod.apply_columns(*pos, reduced, coarse);
            }
        }
    }

    /// The session, while one streams far terrain.
    pub(super) fn lss_active(&self) -> Option<&Client> {
        self.remote
            .as_ref()
            .and_then(|r| r.lss.as_ref())
            .filter(|c| c.active())
    }

    pub(super) fn lss_status(&self) -> serde_json::Value {
        let Some(client) = self.remote.as_ref().and_then(|r| r.lss.as_ref()) else {
            return serde_json::Value::Null;
        };
        let stats = client.stats();
        serde_json::json!({
            "active": client.active(),
            "distance": client.distance(),
            "server_distance": client.server_distance(),
            "known": stats.session.known,
            "received": stats.session.received,
            "up_to_date": stats.session.up_to_date,
            "not_generated": stats.session.not_generated,
            "in_flight": stats.session.in_flight,
            "confirmed_ring": stats.session.confirmed_ring,
            "backlog": stats.backlog,
            "stored": stats.stored,
        })
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn server_folders_are_plain_names() {
        assert_eq!(
            super::folder("play.example.net:25565"),
            "play.example.net_25565"
        );
        assert_eq!(super::folder("[::1]:25565"), "___1__25565");
    }
}
