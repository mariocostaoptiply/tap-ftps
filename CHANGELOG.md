# Changelog

## Unreleased
  * Accept flat optional filename and replication-column fields for each of the nine Optiply streams, without a list-based UI.
  * Skip unconfigured flat streams and reject mixing flat filenames with legacy tables.
  * Add exact `file_name` selection under a shared `files_path`, without recursive searching.
  * Keep legacy regex selection; `search_prefix` can default to the shared folder and `search_pattern` is optional with an exact filename.
  * Preserve stream identities and incremental bookmarks in discovery and sync.
  * Exclude directories from nonrecursive file listings.

## 1.0.2
  * If paramiko returns to us a null `st_mtime` for a file then default to utcnow to force the file to sync
