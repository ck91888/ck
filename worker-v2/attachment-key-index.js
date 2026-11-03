import {ensureSchema} from './schema-ready.js';
export const ATTACHMENT_KEY_INDEX='CREATE INDEX IF NOT EXISTS idx_v2_attachments_file_key ON v2_attachments(file_key)';
// Additive, non-unique DDL only. Existing rows, duplicate keys and rowids remain
// intact. Run after base migrations, without replaying their business triggers.
// A failed batch is not cached; independent cold bindings safely converge.
export const ensureAttachmentKeyIndex=db=>ensureSchema(db,'attachment-file-key-index-v1',()=>db.batch([db.prepare(ATTACHMENT_KEY_INDEX)]));
