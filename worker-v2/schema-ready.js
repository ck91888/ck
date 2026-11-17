// Cache only successful schema initialization, scoped to a particular DB binding.
// Never retain request promises, session data, permissions or business records.
const ready = new WeakMap();
const bindings = new WeakMap();
// DB decorators share only successful DDL readiness with their actual binding.
// Sessions, permissions, flags, results and in-flight promises are never shared.
export function shareSchemaBinding(wrapper,source){bindings.set(wrapper,bindings.get(source)||source);}
export async function ensureSchema(db, version, initialize) {
  const binding=bindings.get(db)||db;
  if (ready.get(binding)?.has(version)) return;
  await initialize();
  let versions = ready.get(binding);
  if (!versions) ready.set(binding, versions = new Set());
  versions.add(version);
}
