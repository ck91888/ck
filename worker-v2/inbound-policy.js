// One rollout predicate for recognition, batch preparation and transactional
// dispatch admission. Request payloads never supply these environment values.
export const inboundFlowEnabled=env=>env.SOP_ENVIRONMENT==='staging'&&env.SOP_UPGRADE_ENABLED==='true';
