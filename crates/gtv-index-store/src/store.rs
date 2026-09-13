//! Versioned index store with atomic activation (shadow build / swap / rollback).
//!
//! ```text
//! root/
//!   indexes.json                 # name -> IndexId
//!   <index_id>/
//!     CURRENT                    # active version number
//!     v1/index.gtvidx  manifest.json
//!     v2/index.gtvidx  manifest.json
//! ```
//!
//! `build` always writes a **new** `v<n>/` and only moves `CURRENT` when
//! `activate`/`save` is called, so a shadow build never disturbs live readers;
//! `rollback` is just `activate` of an older version.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use gtv_catalog::{DataFileId, IndexId, SnapshotId, TableId};
use gtv_core::VectorIndex;
use gtv_index::{AnyIndex, BuildOptions};
use gtv_storage::write_atomic;

use crate::container::{decode, encode};
use crate::manifest::IndexManifest;
use crate::{IndexStoreError, Result};

/// Caller-supplied provenance for an index build (all optional except the model).
#[derive(Debug, Clone, Default)]
pub struct IndexMeta {
    pub table_id: Option<TableId>,
    pub corpus_snapshot_id: Option<SnapshotId>,
    pub source_file_ids: Vec<DataFileId>,
    pub model_id: String,
    pub model_version: String,
    pub embedding_model: String,
    pub feature_version: String,
    pub normalized: bool,
    pub tombstone_count: u64,
}

/// Identifies one built version of an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexVersion {
    pub index_id: IndexId,
    pub version: u32,
}

/// A loaded index together with its manifest.
#[derive(Debug, Clone)]
pub struct LoadedIndex {
    pub manifest: IndexManifest,
    pub index: AnyIndex,
}

/// Filesystem-backed, versioned index store.
#[derive(Debug, Clone)]
pub struct IndexStore {
    root: PathBuf,
}

impl IndexStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    // -- paths --------------------------------------------------------------

    fn registry_path(&self) -> PathBuf {
        self.root.join("indexes.json")
    }
    fn index_dir(&self, id: IndexId) -> PathBuf {
        self.root.join(id.to_string())
    }
    fn version_dir(&self, id: IndexId, v: u32) -> PathBuf {
        self.index_dir(id).join(format!("v{v}"))
    }
    fn container_path(&self, id: IndexId, v: u32) -> PathBuf {
        self.version_dir(id, v).join("index.gtvidx")
    }
    fn manifest_path(&self, id: IndexId, v: u32) -> PathBuf {
        self.version_dir(id, v).join("manifest.json")
    }
    fn current_path(&self, id: IndexId) -> PathBuf {
        self.index_dir(id).join("CURRENT")
    }

    // -- registry -----------------------------------------------------------

    fn load_registry(&self) -> Result<BTreeMap<String, IndexId>> {
        match fs::read(self.registry_path()) {
            Ok(b) => Ok(serde_json::from_slice(&b)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
            Err(e) => Err(e.into()),
        }
    }

    fn save_registry(&self, reg: &BTreeMap<String, IndexId>) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(reg)?;
        write_atomic(&self.registry_path(), &bytes)?;
        Ok(())
    }

    /// Resolve an index name to its id.
    pub fn resolve(&self, name: &str) -> Result<IndexId> {
        self.load_registry()?
            .get(name)
            .copied()
            .ok_or_else(|| IndexStoreError::NotFound(name.to_string()))
    }

    fn resolve_or_create(&self, name: &str) -> Result<IndexId> {
        let mut reg = self.load_registry()?;
        if let Some(id) = reg.get(name) {
            return Ok(*id);
        }
        let id = IndexId::new();
        reg.insert(name.to_string(), id);
        fs::create_dir_all(self.index_dir(id))?;
        self.save_registry(&reg)?;
        Ok(id)
    }

    /// All registered indexes as `(name, id)`.
    pub fn list(&self) -> Result<Vec<(String, IndexId)>> {
        Ok(self.load_registry()?.into_iter().collect())
    }

    /// Built versions of an index, ascending.
    pub fn versions(&self, name: &str) -> Result<Vec<u32>> {
        let id = self.resolve(name)?;
        let mut out = Vec::new();
        if let Ok(rd) = fs::read_dir(self.index_dir(id)) {
            for entry in rd.flatten() {
                let fname = entry.file_name().to_string_lossy().into_owned();
                if let Some(rest) = fname.strip_prefix('v') {
                    if let Ok(v) = rest.parse::<u32>() {
                        out.push(v);
                    }
                }
            }
        }
        out.sort_unstable();
        Ok(out)
    }

    /// The active version of an index, if any.
    pub fn current_version(&self, name: &str) -> Result<Option<u32>> {
        let id = self.resolve(name)?;
        match fs::read_to_string(self.current_path(id)) {
            Ok(s) => Ok(Some(
                s.trim()
                    .parse()
                    .map_err(|_| IndexStoreError::Corrupt("bad CURRENT pointer".into()))?,
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // -- build / activate ---------------------------------------------------

    /// Write a new version (shadow build). Does not change `CURRENT` unless this
    /// is the first version.
    pub fn build(
        &self,
        name: &str,
        index: &AnyIndex,
        options: &BuildOptions,
        meta: &IndexMeta,
    ) -> Result<IndexVersion> {
        let id = self.resolve_or_create(name)?;
        let version = self.versions(name)?.last().copied().unwrap_or(0) + 1;
        fs::create_dir_all(self.version_dir(id, version))?;

        let payload = index.to_bytes();
        let payload_checksum = blake3::hash(&payload).to_hex().to_string();
        let manifest = IndexManifest {
            index_id: id,
            name: name.to_string(),
            table_id: meta.table_id,
            corpus_snapshot_id: meta.corpus_snapshot_id,
            source_file_ids: meta.source_file_ids.clone(),
            index_type: index.index_type(),
            model_id: meta.model_id.clone(),
            model_version: meta.model_version.clone(),
            embedding_model: meta.embedding_model.clone(),
            feature_version: meta.feature_version.clone(),
            normalized: meta.normalized,
            dim: index.dim() as u32,
            metric: index.metric().as_str().to_string(),
            build_options: options.clone(),
            build_ts: gtv_catalog::schema::now_ns(),
            payload_checksum,
            engine_version: env!("CARGO_PKG_VERSION").to_string(),
            row_count: index.len() as u64,
            tombstone_count: meta.tombstone_count,
        };

        let manifest_json = serde_json::to_vec_pretty(&manifest)?;
        let container = encode(&manifest_json, &payload);
        write_atomic(&self.container_path(id, version), &container)?;
        write_atomic(&self.manifest_path(id, version), &manifest_json)?;

        // First version becomes active automatically.
        let v = IndexVersion { index_id: id, version };
        if self.current_version(name)?.is_none() {
            self.activate(name, version)?;
        }
        Ok(v)
    }

    /// Point `CURRENT` at `version` (atomic swap / rollback).
    pub fn activate(&self, name: &str, version: u32) -> Result<()> {
        let id = self.resolve(name)?;
        if !self.container_path(id, version).exists() {
            return Err(IndexStoreError::VersionNotFound {
                name: name.to_string(),
                version,
            });
        }
        write_atomic(&self.current_path(id), version.to_string().as_bytes())?;
        Ok(())
    }

    /// Build and immediately activate a new version.
    pub fn save(
        &self,
        name: &str,
        index: &AnyIndex,
        options: &BuildOptions,
        meta: &IndexMeta,
    ) -> Result<IndexVersion> {
        let v = self.build(name, index, options, meta)?;
        self.activate(name, v.version)?;
        Ok(v)
    }

    /// Alias for [`IndexStore::activate`] on an older version.
    pub fn rollback(&self, name: &str, version: u32) -> Result<()> {
        self.activate(name, version)
    }

    // -- load / verify ------------------------------------------------------

    /// Load an index version (`None` = the active one), verifying the container
    /// and payload checksums.
    pub fn load(&self, name: &str, version: Option<u32>) -> Result<LoadedIndex> {
        let id = self.resolve(name)?;
        let v = match version {
            Some(v) => v,
            None => self
                .current_version(name)?
                .ok_or_else(|| IndexStoreError::NotFound(name.to_string()))?,
        };
        let path = self.container_path(id, v);
        let bytes = fs::read(&path).map_err(|_| IndexStoreError::VersionNotFound {
            name: name.to_string(),
            version: v,
        })?;
        let (manifest_json, payload) = decode(&bytes)?;
        let manifest: IndexManifest = serde_json::from_slice(&manifest_json)?;
        if blake3::hash(&payload).to_hex().to_string() != manifest.payload_checksum {
            return Err(IndexStoreError::Corrupt(format!(
                "payload checksum mismatch for {name} v{v}"
            )));
        }
        let index = AnyIndex::from_bytes(manifest.index_type, &payload)?;
        Ok(LoadedIndex { manifest, index })
    }

    /// Read only the manifest of a version (no payload decode).
    pub fn manifest(&self, name: &str, version: Option<u32>) -> Result<IndexManifest> {
        let id = self.resolve(name)?;
        let v = match version {
            Some(v) => v,
            None => self
                .current_version(name)?
                .ok_or_else(|| IndexStoreError::NotFound(name.to_string()))?,
        };
        let bytes = fs::read(self.manifest_path(id, v)).map_err(|_| {
            IndexStoreError::VersionNotFound {
                name: name.to_string(),
                version: v,
            }
        })?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// Verify container + payload checksums and that the payload decodes.
    pub fn verify(&self, name: &str, version: u32) -> Result<()> {
        let loaded = self.load(name, Some(version))?;
        if loaded.index.len() as u64 != loaded.manifest.row_count {
            return Err(IndexStoreError::Corrupt(format!(
                "row count mismatch for {name} v{version}"
            )));
        }
        Ok(())
    }
}
