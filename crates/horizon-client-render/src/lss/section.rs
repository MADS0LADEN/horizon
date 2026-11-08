//! V20 LSS section-array body → [`crate::chunk::Chunk`].
use crate::biome::Biomes;
use crate::chunk::{self, BIOME_STRATEGY, Chunk, ColumnLayout, Light, Section, share_uniform};
use crate::fast_hash::FxHashMap;
use horizon_core::paletted::PalettedContainer;
use horizon_protocol::buf::Reader;
use horizon_world::block_state_properties::BlockProperties;
use horizon_world::chunk::BlockState;
use horizon_world::registry::BlockRegistry;
use std::collections::BTreeMap;
use std::sync::Arc;

const BLOCK_ENTRIES: usize = 4096;
const BIOME_ENTRIES: usize = 64;
const MAX_IDENTITY_BYTES: usize = 4096;
const BLOCK_MIN_BITS: u8 = 4;
const BLOCK_MAX_BITS: u8 = 12;
const BIOME_MAX_BITS: u8 = 6;
const LIGHT_BYTES: usize = 2048;

fn map_err(e: horizon_protocol::Error) -> String {
    e.to_string()
}

fn read_identity(r: &mut Reader<'_>) -> Result<String, String> {
    let len = r.count(MAX_IDENTITY_BYTES).map_err(map_err)?;
    let bytes = r.bytes(len).map_err(map_err)?;
    std::str::from_utf8(bytes)
        .map_err(|e| format!("bad UTF-8: {e}"))
        .map(str::to_owned)
}

fn check_block_width(bits: u8) -> Result<(), String> {
    if bits == 0 || (BLOCK_MIN_BITS..=BLOCK_MAX_BITS).contains(&bits) {
        Ok(())
    } else {
        Err(format!("block container width {bits} is illegal"))
    }
}

fn check_biome_width(bits: u8) -> Result<(), String> {
    if bits <= BIOME_MAX_BITS {
        Ok(())
    } else {
        Err(format!(
            "biome container width {bits} exceeds max {BIOME_MAX_BITS}"
        ))
    }
}

fn read_palette_value(r: &mut Reader<'_>, dict_size: usize, what: &str) -> Result<usize, String> {
    let v = r.varint().map_err(map_err)?;
    if v < 0 {
        return Err(format!("{what} palette value {v} is negative"));
    }
    let v = v as usize;
    if v >= dict_size {
        return Err(format!(
            "{what} palette index {v} outside dictionary of {dict_size}"
        ));
    }
    Ok(v)
}

fn read_container(
    r: &mut Reader<'_>,
    entries: usize,
    is_blocks: bool,
    dict_size: usize,
) -> Result<(u8, Vec<usize>, Vec<i64>), String> {
    let bits = r.u8().map_err(map_err)?;
    if is_blocks {
        check_block_width(bits)?;
    } else {
        check_biome_width(bits)?;
    }
    if bits == 0 {
        let what = if is_blocks { "block" } else { "biome" };
        let idx = read_palette_value(r, dict_size, what)?;
        return Ok((0, vec![idx], Vec::new()));
    }
    let palette_count = r.varint().map_err(map_err)?;
    if palette_count <= 0 || palette_count as usize > entries {
        return Err(format!(
            "palette count {palette_count} outside [1, {entries}]"
        ));
    }
    let palette_count = palette_count as usize;
    let what = if is_blocks { "block" } else { "biome" };
    let mut palette = Vec::with_capacity(palette_count);
    for _ in 0..palette_count {
        palette.push(read_palette_value(r, dict_size, what)?);
    }
    let values_per_long = 64 / bits as usize;
    let long_count = entries.div_ceil(values_per_long);
    if r.remaining() < long_count * 8 {
        return Err("container data past the end".into());
    }
    let mut data = Vec::with_capacity(long_count);
    for _ in 0..long_count {
        data.push(r.i64().map_err(map_err)?);
    }
    Ok((bits, palette, data))
}

fn unpack_indices(
    data: &[i64],
    bits: u8,
    entries: usize,
    palette_len: usize,
) -> Result<Vec<usize>, String> {
    if bits == 0 {
        return Err("unpack with zero width".into());
    }
    let bits = bits as usize;
    let values_per_long = 64 / bits;
    let long_count = entries.div_ceil(values_per_long);
    if data.len() != long_count {
        return Err(format!(
            "container data has {} longs, expected {}",
            data.len(),
            long_count
        ));
    }
    let mask = (1u64 << bits) - 1;
    let mut out = Vec::with_capacity(entries);
    for i in 0..entries {
        let word = data[i / values_per_long];
        let shift = (i % values_per_long) * bits;
        let v = ((word >> shift) & mask as i64) as usize;
        if v >= palette_len {
            return Err(format!("packed value {v} >= palette size {palette_len}"));
        }
        out.push(v);
    }
    Ok(out)
}

fn resolve_container(
    bits: u8,
    palette: &[usize],
    data: &[i64],
    entries: usize,
    dict: &[String],
    dict_cache: &mut [Option<u32>],
    block: &mut impl FnMut(&str) -> u32,
) -> Result<Vec<u32>, String> {
    if bits == 0 {
        let idx = *palette.first().ok_or("empty single-value palette")?;
        let id = dict_entry_id(dict, dict_cache, idx, block)?;
        return Ok(vec![id; entries]);
    }
    let indices = unpack_indices(data, bits, entries, palette.len())?;
    let mut values = Vec::with_capacity(entries);
    for &pal in &indices {
        let idx = palette[pal];
        values.push(dict_entry_id(dict, dict_cache, idx, block)?);
    }
    Ok(values)
}

fn resolve_biome_container(
    bits: u8,
    palette: &[usize],
    data: &[i64],
    dict: &[String],
    dict_cache: &mut [Option<u16>],
    biome: &mut impl FnMut(&str) -> u16,
) -> Result<Vec<u32>, String> {
    let entries = BIOME_ENTRIES;
    if bits == 0 {
        let idx = *palette.first().ok_or("empty single-value palette")?;
        let id = dict_entry_biome(dict, dict_cache, idx, biome)?;
        return Ok(vec![u32::from(id); entries]);
    }
    let indices = unpack_indices(data, bits, entries, palette.len())?;
    let mut values = Vec::with_capacity(entries);
    for &pal in &indices {
        let idx = palette[pal];
        values.push(u32::from(dict_entry_biome(dict, dict_cache, idx, biome)?));
    }
    Ok(values)
}

fn dict_entry_id(
    dict: &[String],
    cache: &mut [Option<u32>],
    idx: usize,
    block: &mut impl FnMut(&str) -> u32,
) -> Result<u32, String> {
    if let Some(id) = cache[idx] {
        return Ok(id);
    }
    let id = block(&dict[idx]);
    cache[idx] = Some(id);
    Ok(id)
}

fn dict_entry_biome(
    dict: &[String],
    cache: &mut [Option<u16>],
    idx: usize,
    biome: &mut impl FnMut(&str) -> u16,
) -> Result<u16, String> {
    if let Some(id) = cache[idx] {
        return Ok(id);
    }
    let id = biome(&dict[idx]);
    cache[idx] = Some(id);
    Ok(id)
}

fn nibble(data: &[u8], index: usize) -> u8 {
    (data[index >> 1] >> ((index & 1) * 4)) & 15
}

fn section_light(
    block_layer: Option<&[u8]>,
    sky_layer: Option<&[u8]>,
    sky_light: bool,
    above: &mut [u8; 256],
) -> Light {
    let mut packed = [0u8; BLOCK_ENTRIES];
    for (i, value) in packed.iter_mut().enumerate() {
        let sky = if sky_light {
            sky_layer.map_or(above[i & 255], |data| nibble(data, i))
        } else {
            0
        };
        let block = block_layer.map_or(0, |data| nibble(data, i));
        *value = sky << 4 | block;
    }
    for (index, value) in above.iter_mut().enumerate() {
        *value = packed[index] >> 4;
    }
    if packed.iter().all(|&b| b == packed[0]) {
        Light::Uniform(packed[0])
    } else {
        Light::Full(Box::new(packed))
    }
}

struct WireSection {
    block_layer: Option<Vec<u8>>,
    sky_layer: Option<Vec<u8>>,
    blocks: Vec<u32>,
    biomes: Vec<u32>,
}

pub fn decode(
    body: &[u8],
    pos: (i32, i32),
    layout: ColumnLayout,
    sky_light: bool,
    block: &mut impl FnMut(&str) -> u32,
    biome: &mut impl FnMut(&str) -> u16,
) -> Result<Chunk, String> {
    let mut r = Reader::new(body);
    let dict_count = r.varint().map_err(map_err)?;
    if dict_count < 0 {
        return Err(format!("negative dictCount {dict_count}"));
    }
    let dict_count = dict_count as usize;
    if dict_count > r.remaining() {
        return Err(format!("dictCount {dict_count} exceeds remaining bytes"));
    }
    let mut dict = Vec::with_capacity(dict_count);
    for _ in 0..dict_count {
        dict.push(read_identity(&mut r)?);
    }
    let section_count = r.varint().map_err(map_err)?;
    if section_count < 0 {
        return Err(format!("negative sectionCount {section_count}"));
    }
    let section_count = section_count as usize;
    if (section_count == 0) != dict.is_empty() {
        return Err(format!(
            "clear-column invariant violated: dictCount={} sectionCount={section_count}",
            dict.len()
        ));
    }
    if section_count > r.remaining() {
        return Err(format!(
            "sectionCount {section_count} exceeds remaining bytes"
        ));
    }

    let mut dict_block_cache = vec![None; dict.len()];
    let mut dict_biome_cache = vec![None; dict.len()];
    let mut wire_sections: FxHashMap<i8, WireSection> = FxHashMap::default();

    for _ in 0..section_count {
        let section_y = r.i8().map_err(map_err)?;
        let _non_empty = r.i16().map_err(map_err)?;
        let _fluid = r.i16().map_err(map_err)?;
        let (block_bits, block_palette, block_data) =
            read_container(&mut r, BLOCK_ENTRIES, true, dict.len())?;
        let (biome_bits, biome_palette, biome_data) =
            read_container(&mut r, BIOME_ENTRIES, false, dict.len())?;
        let has_block = r.bool().map_err(map_err)?;
        let block_layer = if has_block {
            Some(r.bytes(LIGHT_BYTES).map_err(map_err)?.to_vec())
        } else {
            None
        };
        let has_sky = r.bool().map_err(map_err)?;
        let sky_layer = if has_sky {
            Some(r.bytes(LIGHT_BYTES).map_err(map_err)?.to_vec())
        } else {
            None
        };
        let blocks = resolve_container(
            block_bits,
            &block_palette,
            &block_data,
            BLOCK_ENTRIES,
            &dict,
            &mut dict_block_cache,
            block,
        )?;
        let biomes = resolve_biome_container(
            biome_bits,
            &biome_palette,
            &biome_data,
            &dict,
            &mut dict_biome_cache,
            biome,
        )?;
        wire_sections.insert(
            section_y,
            WireSection {
                block_layer,
                sky_layer,
                blocks,
                biomes,
            },
        );
    }
    if r.remaining() != 0 {
        return Err(format!(
            "{} trailing bytes after section array",
            r.remaining()
        ));
    }

    let mut chunk = Chunk::with_layout(pos.0, pos.1, layout);
    let mut above = [if sky_light { 15u8 } else { 0u8 }; 256];
    for si in (0..layout.sections()).rev() {
        let section_coord = layout.section_y(si) >> 4;
        if let Some(ws) = wire_sections.get(&(section_coord as i8)) {
            let blocks: [u32; BLOCK_ENTRIES] = ws
                .blocks
                .clone()
                .try_into()
                .map_err(|_| "internal block value count".to_string())?;
            let biomes64: [u32; BIOME_ENTRIES] = ws
                .biomes
                .clone()
                .try_into()
                .map_err(|_| "internal biome value count".to_string())?;
            let light = section_light(
                ws.block_layer.as_deref(),
                ws.sky_layer.as_deref(),
                sky_light,
                &mut above,
            );
            chunk.sections[si] = Arc::new(Section {
                blocks: Section::from_blocks(&blocks),
                biomes: PalettedContainer::from_values(BIOME_STRATEGY, &biomes64),
                light,
            });
        } else {
            let light = section_light(None, None, sky_light, &mut above);
            chunk.sections[si] = Arc::new(Section {
                blocks: PalettedContainer::new(chunk::block_strategy(), 0),
                biomes: PalettedContainer::new(BIOME_STRATEGY, 0),
                light,
            });
        }
    }
    share_uniform(&mut chunk.sections);
    Ok(chunk)
}

pub struct Resolver {
    registry: &'static BlockRegistry,
    biomes: Arc<Biomes>,
    blocks: FxHashMap<String, u32>,
    biome_cache: FxHashMap<String, u16>,
}

impl Resolver {
    pub fn new(registry: &'static BlockRegistry, biomes: Arc<Biomes>) -> Self {
        Self {
            registry,
            biomes,
            blocks: FxHashMap::default(),
            biome_cache: FxHashMap::default(),
        }
    }

    pub fn block(&mut self, identity: &str) -> u32 {
        if let Some(&id) = self.blocks.get(identity) {
            return id;
        }
        let id = resolve_block_identity(self.registry, identity);
        self.blocks.insert(identity.to_owned(), id);
        id
    }

    pub fn biome(&mut self, identity: &str) -> u16 {
        if let Some(&id) = self.biome_cache.get(identity) {
            return id;
        }
        let id = self.biomes.id(identity);
        self.biome_cache.insert(identity.to_owned(), id);
        id
    }
}

fn parse_identity(identity: &str) -> Option<(String, BTreeMap<String, String>)> {
    let (name, props) = identity.split_once('[')?;
    let mut properties = BTreeMap::new();
    let inner = props.strip_suffix(']')?;
    if inner.is_empty() {
        return Some((name.to_owned(), properties));
    }
    for part in inner.split(',') {
        let (k, v) = part.split_once('=')?;
        properties.insert(k.to_owned(), v.to_owned());
    }
    Some((name.to_owned(), properties))
}

fn resolve_block_identity(registry: &BlockRegistry, identity: &str) -> u32 {
    let state = if let Some((name, properties)) = parse_identity(identity) {
        BlockState {
            name,
            properties: properties.into(),
        }
    } else {
        BlockState {
            name: identity.to_owned(),
            properties: BlockProperties::new(),
        }
    };
    if let Some(id) = registry.id(&state) {
        return id.0;
    }
    let Some(default_id) = registry.default_state(&state.name) else {
        return registry
            .default_state("minecraft:stone")
            .map_or(0, |id| id.0);
    };
    let Some(default) = registry.state(default_id) else {
        return default_id.0;
    };
    let mut merged = default.clone();
    for (key, value) in &state.properties {
        if !merged.properties.contains_key(key) {
            continue;
        }
        if registry
            .property_values(&state.name, key)
            .contains(&value.as_str())
        {
            merged.properties.insert(key.clone(), value.clone());
        }
    }
    registry
        .id(&merged)
        .or(Some(default_id))
        .map(|id| id.0)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunk::index;
    use horizon_protocol::buf::Writer;

    fn intern_block() -> impl FnMut(&str) -> u32 + Clone {
        let mut table: Vec<String> = Vec::new();
        move |name: &str| -> u32 {
            if let Some(i) = table.iter().position(|s| s == name) {
                return i as u32;
            }
            table.push(name.to_owned());
            table.len() as u32 - 1
        }
    }

    fn intern_biome() -> impl FnMut(&str) -> u16 + Clone {
        let mut table: Vec<String> = Vec::new();
        move |name: &str| -> u16 {
            if let Some(i) = table.iter().position(|s| s == name) {
                return i as u16;
            }
            table.push(name.to_owned());
            table.len() as u16 - 1
        }
    }

    fn decode_fixture(body: &[u8]) -> Chunk {
        let mut block = intern_block();
        let mut biome = intern_biome();
        decode(
            body,
            (0, 0),
            ColumnLayout::VANILLA,
            true,
            &mut block,
            &mut biome,
        )
        .unwrap()
    }

    #[test]
    fn fixture_columns_decode() {
        for (name, body) in [
            ("col-00", include_bytes!("testdata/col-00.bin").as_slice()),
            (
                "negative-y",
                include_bytes!("testdata/negative-y.bin").as_slice(),
            ),
            (
                "multi-section",
                include_bytes!("testdata/multi-section.bin").as_slice(),
            ),
            (
                "light-combos",
                include_bytes!("testdata/light-combos.bin").as_slice(),
            ),
            (
                "biome-three-bit",
                include_bytes!("testdata/biome-three-bit.bin").as_slice(),
            ),
            (
                "waterlogged",
                include_bytes!("testdata/waterlogged.bin").as_slice(),
            ),
        ] {
            decode_fixture(body);
            let _ = name;
        }
    }

    #[test]
    fn col_00_bedrock_and_sky() {
        let mut block = intern_block();
        let bedrock = block("minecraft:bedrock");
        let mut biome = intern_biome();
        let chunk = decode(
            include_bytes!("testdata/col-00.bin"),
            (0, 0),
            ColumnLayout::VANILLA,
            true,
            &mut block,
            &mut biome,
        )
        .unwrap();
        let bottom = &chunk.sections[0];
        for x in 0..16 {
            for z in 0..16 {
                assert_eq!(
                    bottom.get(index(x, 0, z)),
                    bedrock,
                    "bedrock at y=-64 column ({x},{z})"
                );
            }
        }
        let mut saw_sky = false;
        for si in 0..chunk.sections.len() {
            if layout_section_bottom_y(si) <= 0 {
                continue;
            }
            let section = &chunk.sections[si];
            for x in 0..16 {
                for z in 0..16 {
                    let top = index(x, 15, z);
                    if section.light(top) >> 4 == 15 {
                        saw_sky = true;
                    }
                }
            }
        }
        assert!(saw_sky, "expected sky 15 in a section above y=0");
    }

    fn layout_section_bottom_y(si: usize) -> i32 {
        ColumnLayout::VANILLA.section_y(si)
    }

    #[test]
    fn negative_y_places_blocks() {
        let chunk = decode_fixture(include_bytes!("testdata/negative-y.bin"));
        let mut non_air = 0u32;
        for section in &chunk.sections {
            let mut buf = [0u32; 4096];
            section.unpack(&mut buf);
            non_air += buf.iter().filter(|&&v| v != 0).count() as u32;
        }
        assert!(non_air > 0, "expected blocks in negative sections");
    }

    #[test]
    fn clear_column_all_air() {
        let mut w = Writer::new();
        w.varint(0);
        w.varint(0);
        let chunk = decode_fixture(&w.data);
        for section in &chunk.sections {
            assert!(section.is_uniform(0));
        }
    }

    fn write_single_block_section(w: &mut Writer, section_y: i8, dict_idx: i32) {
        w.i8(section_y);
        w.i16(1);
        w.i16(0);
        w.u8(0);
        w.varint(dict_idx);
        w.u8(0);
        w.varint(dict_idx);
        w.bool(false);
        w.bool(false);
    }

    #[test]
    fn bits_zero_single_value_section() {
        let mut w = Writer::new();
        w.varint(1);
        w.string("minecraft:stone");
        w.varint(1);
        write_single_block_section(&mut w, 0, 0);
        let chunk = decode_fixture(&w.data);
        assert!(chunk.sections[layout_section_index(0)].is_uniform(0));
    }

    fn layout_section_index(section_coord: i8) -> usize {
        ColumnLayout::VANILLA
            .section_index(i32::from(section_coord) * 16)
            .unwrap()
    }

    #[test]
    fn four_bit_palette_no_span() {
        let mut w = Writer::new();
        w.varint(2);
        w.string("minecraft:stone");
        w.string("minecraft:dirt");
        w.varint(1);
        w.i8(0);
        w.i16(4096);
        w.i16(0);
        w.u8(4);
        w.varint(2);
        w.varint(0);
        w.varint(1);
        let long_count = BLOCK_ENTRIES.div_ceil(64 / 4);
        for _ in 0..long_count {
            w.i64(0x1111_1111_1111_1111);
        }
        w.u8(0);
        w.varint(0);
        w.bool(false);
        w.bool(false);
        decode_fixture(&w.data);
    }

    #[test]
    fn five_bit_palette_no_span() {
        let mut w = Writer::new();
        w.varint(1);
        w.string("minecraft:grass_block");
        w.varint(1);
        w.i8(0);
        w.i16(64);
        w.i16(0);
        w.u8(5);
        w.varint(1);
        w.varint(0);
        let long_count = BLOCK_ENTRIES.div_ceil(64 / 5);
        for _ in 0..long_count {
            w.i64(0);
        }
        w.u8(0);
        w.varint(0);
        w.bool(false);
        w.bool(false);
        decode_fixture(&w.data);
    }

    #[test]
    fn rejects_block_width_three() {
        let mut w = Writer::new();
        w.varint(1);
        w.string("minecraft:stone");
        w.varint(1);
        w.i8(0);
        w.i16(0);
        w.i16(0);
        w.u8(3);
        let mut block = intern_block();
        let mut biome = intern_biome();
        assert!(
            decode(
                &w.data,
                (0, 0),
                ColumnLayout::VANILLA,
                true,
                &mut block,
                &mut biome
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_palette_index_outside_dict() {
        let mut w = Writer::new();
        w.varint(1);
        w.string("minecraft:stone");
        w.varint(1);
        w.i8(0);
        w.i16(0);
        w.i16(0);
        w.u8(0);
        w.varint(1);
        let mut block = intern_block();
        let mut biome = intern_biome();
        assert!(
            decode(
                &w.data,
                (0, 0),
                ColumnLayout::VANILLA,
                true,
                &mut block,
                &mut biome
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_trailing_byte() {
        let mut w = Writer::new();
        w.varint(0);
        w.varint(0);
        w.u8(0);
        let mut block = intern_block();
        let mut biome = intern_biome();
        assert!(
            decode(
                &w.data,
                (0, 0),
                ColumnLayout::VANILLA,
                true,
                &mut block,
                &mut biome
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_sections_without_dictionary() {
        let mut w = Writer::new();
        w.varint(0);
        w.varint(1);
        let mut block = intern_block();
        let mut biome = intern_biome();
        assert!(
            decode(
                &w.data,
                (0, 0),
                ColumnLayout::VANILLA,
                true,
                &mut block,
                &mut biome
            )
            .is_err()
        );
    }
}
