// Cache only successful schema initialization, scoped to a particular DB binding.
// Never retain request promises, session data, permissions or business records.
const ready = new WeakMap();
export async function ensureSchema(db, version, initialize) {
  if (ready.get(db)?.has(version)) return;
  await initialize();
  let versions = ready.get(db);
  if (!versions) ready.set(db, versions = new Set());
  versions.add(version);
}
