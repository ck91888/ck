// Cache only successful schema initialization, scoped to a particular DB binding.
// Share only in-flight schema initialization; never retain sessions or business data.
const ready = new WeakMap();
const pending = new WeakMap();
export async function ensureSchema(db, version, initialize) {
  if (ready.get(db)?.has(version)) return;
  let work = pending.get(db);
  if (work?.has(version)) return work.get(version);
  if (!work) pending.set(db, work = new Map());
  const task = Promise.resolve().then(initialize).then(() => {
    let versions = ready.get(db);
    if (!versions) ready.set(db, versions = new Set());
    versions.add(version);
  }).finally(() => {
    work.delete(version);
    if (!work.size) pending.delete(db);
  });
  work.set(version, task);
  return task;
}
