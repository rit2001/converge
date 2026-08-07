import { readdir, readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";

const databaseUrl = process.env.DATABASE_URL;
if (!databaseUrl) throw new Error("DATABASE_URL is required");

const pool = new pg.Pool({ connectionString: databaseUrl, max: 1 });
const directory = join(dirname(fileURLToPath(import.meta.url)), "../migrations");
const files = (await readdir(directory)).filter((file) => file.endsWith(".sql")).sort();

try {
  for (const file of files) {
    const exists = await pool.query<{ exists: boolean }>(
      "SELECT to_regclass('converge_migrations') IS NOT NULL AS exists",
    );
    if (exists.rows[0]?.exists) {
      const applied = await pool.query("SELECT 1 FROM converge_migrations WHERE name = $1", [file]);
      if (applied.rowCount) continue;
    }
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(await readFile(join(directory, file), "utf8"));
      await client.query(
        "INSERT INTO converge_migrations(name) VALUES ($1) ON CONFLICT DO NOTHING",
        [file],
      );
      await client.query("COMMIT");
      process.stdout.write(`Applied ${file}\n`);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
} finally {
  await pool.end();
}
