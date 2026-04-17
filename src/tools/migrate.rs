/// Schema migrations for the S3 search index cache database.
///
/// Each entry maps a source version to the SQL statements that migrate
/// to the next version. Migrations run in order until user_version is current.
/// Since this is a cache, data loss on failed migration is acceptable —
/// the db is deleted and recreated from scratch.

const S3_MIGRATIONS: &[(u32, &[&str])] = &[
    // 0 → 1: initial schema
    (0, &[
        "CREATE TABLE files (s3_key TEXT PRIMARY KEY, etag TEXT, size INTEGER, last_modified TEXT)",
        "CREATE TABLE chunks (id INTEGER PRIMARY KEY AUTOINCREMENT, s3_key TEXT NOT NULL, chunk_offset INTEGER, chunk_text TEXT NOT NULL, embedding F32_BLOB(1536) NOT NULL)",
        "CREATE INDEX idx_chunks_s3_key ON chunks(s3_key)",
    ]),
    // 1 → 2: add foreign key with CASCADE delete on chunks
    (1, &[
        "DROP TABLE chunks",
        "CREATE TABLE chunks (id INTEGER PRIMARY KEY AUTOINCREMENT, s3_key TEXT NOT NULL REFERENCES files(s3_key) ON DELETE CASCADE, chunk_offset INTEGER, chunk_text TEXT NOT NULL, embedding F32_BLOB(1536) NOT NULL)",
        "CREATE INDEX idx_chunks_s3_key ON chunks(s3_key)",
    ]),
];

const FILE_MIGRATIONS: &[(u32, &[&str])] = &[
    // 0 → 1: initial schema (path-keyed, mtime-versioned, FK cascade)
    (0, &[
        "CREATE TABLE files (path TEXT PRIMARY KEY, mtime TEXT NOT NULL)",
        "CREATE TABLE chunks (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT NOT NULL REFERENCES files(path) ON DELETE CASCADE, chunk_offset INTEGER, chunk_text TEXT NOT NULL, embedding F32_BLOB(1536) NOT NULL)",
        "CREATE INDEX idx_chunks_path ON chunks(path)",
    ]),
];

/// Run pending migrations on the S3 cache database.
pub async fn run_s3_migrations(
    conn: &libsql::Connection,
    db_path: &std::path::Path,
) -> Result<(), String> {
    run_migrations_with(conn, db_path, S3_MIGRATIONS).await
}

/// Run pending migrations on the local file cache database.
pub async fn run_file_migrations(
    conn: &libsql::Connection,
    db_path: &std::path::Path,
) -> Result<(), String> {
    run_migrations_with(conn, db_path, FILE_MIGRATIONS).await
}

async fn run_migrations_with(
    conn: &libsql::Connection,
    db_path: &std::path::Path,
    migrations: &[(u32, &[&str])],
) -> Result<(), String> {
    match run_migrations(conn, migrations).await {
        Ok(()) => Ok(()),
        Err(e) => {
            tracing::warn!("migration failed ({e}), rebuilding cache");
            drop(std::fs::remove_file(db_path));
            Err(format!("migration failed, cache cleared: {e}"))
        }
    }
}

async fn run_migrations(
    conn: &libsql::Connection,
    migrations: &[(u32, &[&str])],
) -> Result<(), String> {
    let version = get_user_version(conn).await?;
    let target = migrations.iter().map(|(v, _)| v + 1).max().unwrap_or(0);

    if version >= target {
        return Ok(());
    }

    for (from_version, statements) in migrations {
        if version > *from_version {
            continue;
        }
        for sql in *statements {
            conn.execute(sql, ())
                .await
                .map_err(|e| format!("migration v{from_version}: {e}"))?;
        }
    }

    set_user_version(conn, target).await?;
    Ok(())
}

async fn get_user_version(conn: &libsql::Connection) -> Result<u32, String> {
    let mut rows = conn.query("PRAGMA user_version", ())
        .await
        .map_err(|e| format!("read user_version: {e}"))?;
    let row = rows.next().await
        .map_err(|e| format!("read user_version row: {e}"))?
        .ok_or_else(|| "no user_version row".to_string())?;
    let v: i64 = row.get(0)
        .map_err(|e| format!("read user_version value: {e}"))?;
    Ok(v as u32)
}

async fn set_user_version(conn: &libsql::Connection, version: u32) -> Result<(), String> {
    conn.execute(&format!("PRAGMA user_version = {version}"), ())
        .await
        .map_err(|e| format!("set user_version: {e}"))?;
    Ok(())
}
