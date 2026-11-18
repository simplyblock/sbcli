// Talking to labreservation.simplyblock.ai.
//
// Node's own fetch, no dependencies: a JS action here runs from source with
// nothing installed, exactly like lab-lock next door.

const fs = require('node:fs')

const BASE = process.env.LABRES_BASE || 'https://labreservation.simplyblock.ai'
const API = `${BASE}/api/v1`

const input = (name) => process.env[`INPUT_${name.toUpperCase()}`] ?? ''

const warn = (title, message) => console.log(`::warning title=${title}::${message}`)
const fail = (message) => console.log(`::error::${message}`)

const log = (message) => console.log(`[labres] ${message}`)

// Every request. Returns { status, body } and never throws on an HTTP error:
// the service answers failures as JSON with a stable `code`, and branching on
// that is the documented contract.
async function call(method, path, body) {
  const token = process.env.LABRES_TOKEN || ''
  if (!token) throw new Error('LABRES_TOKEN is empty')
  const res = await fetch(`${API}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  })
  let parsed = null
  const text = await res.text()
  if (text) { try { parsed = JSON.parse(text) } catch { parsed = { raw: text } } }
  return { status: res.status, body: parsed ?? {} }
}

// A 409 is the ordinary "the lab is busy" answer, so it should say who has it
// rather than just failing. held_by and free_at are on the cluster, not on the
// error, so this is a second read -- best effort, since a failure to explain
// the failure must not replace it.
async function describeHolder(cluster) {
  if (!cluster) return 'every cluster of that kind is taken'
  try {
    const { status, body } = await call('GET', `/clusters/${encodeURIComponent(cluster)}`)
    if (status !== 200) return `${cluster} is taken`
    const who = body.held_by || 'someone'
    const until = body.free_at ? ` until ${body.free_at}` : ''
    return `${cluster} is held by ${who}${until}`
  } catch {
    return `${cluster} is taken`
  }
}

// The live reservations this key already holds. `GET /reservations` is
// documented as "yours, live ones only", so anything it returns is a cluster
// this account is entitled to use right now.
async function existing(cluster) {
  const rows = await existingAll(cluster)
  return rows[0] || null
}

// Every live reservation this key holds for *cluster*, not just the first.
// A lane can end up holding more than one for the same cluster -- a run that
// died between the 202 and the ready-poll leaves one behind, and the next run
// books another -- and clearing only the first would leave the lab looking
// busy for a reason nothing names.
async function existingAll(cluster) {
  const { status, body } = await call('GET', '/reservations')
  if (status !== 200) return []
  const rows = Array.isArray(body) ? body : (body.reservations || [])
  return rows.filter((r) =>
    (r.state === 'active' || r.state === 'pending') &&
    (!cluster || r.cluster === cluster))
}


async function reserve({ cluster, kind, duration, clean, bootstrap }) {
  const body = { duration }
  if (cluster) body.cluster = cluster
  else body.kind = kind
  // Only send these when true. Unknown or unwanted fields are rejected rather
  // than ignored, and `bootstrap` silently forces `clean` on, so sending
  // false for something we did not ask for is a lie in the log.
  if (clean) body.clean = true
  if (bootstrap) body.bootstrap = true

  log(`requesting ${cluster ? `cluster ${cluster}` : `any free ${kind}`} for ${duration}`)
  const { status, code, ...rest } = await call('POST', '/reservations', body)
    .then((r) => ({ status: r.status, code: r.body.code, body: r.body }))

  if (status === 202 || status === 201) return rest.body
  if (status === 409) {
    throw new Error(`the lab is busy: ${await describeHolder(cluster)}`)
  }
  if (status === 401) {
    throw new Error(
      'the reservation service rejected the token (401). It answers the same ' +
      'for unknown, revoked and wrong, so check the LABRES_TOKEN secret is the ' +
      'whole string including the labres_ prefix, and that it is not revoked.')
  }
  throw new Error(
    `reserve failed: HTTP ${status}${code ? ` ${code}` : ''} ` +
    `${JSON.stringify(rest.body).slice(0, 300)}`)
}

// Poll until ready. The documented rule is to branch on `ready`, and to stop
// the moment `state` leaves pending -- a teardown that keeps failing parks in
// `ending` on purpose and never moves, so waiting for a particular failure
// state waits forever.
async function waitReady(id, timeoutSeconds) {
  const deadline = Date.now() + timeoutSeconds * 1000
  let last = null
  while (Date.now() < deadline) {
    const { status, body } = await call('GET', `/reservations/${id}`)
    if (status !== 200) {
      throw new Error(`reservation ${id} could not be read: HTTP ${status}`)
    }
    if (body.state !== last) { log(`state=${body.state} ready=${body.ready}`); last = body.state }
    if (body.ready === true) return body
    if (body.state !== 'pending') {
      throw new Error(
        `reservation ${id} will not become ready: state=${body.state} ` +
        `end_reason=${body.end_reason || '-'} error=${body.error || '-'}`)
    }
    await new Promise((r) => setTimeout(r, 5000))
  }
  throw new Error(
    `reservation ${id} was still pending after ${timeoutSeconds}s. The ` +
    `authorize playbook runs per host, so a large group is slower, but this ` +
    `long usually means a host in the group is unreachable.`)
}

// The kubeconfig of a k3s cluster this service stood up.
//
// Served only while the reservation asked for `bootstrap` and is still active:
// the service knows about k3s it installed and nothing else, so a cluster
// somebody stood up by hand is invisible here however healthy it is. It reads
// /etc/rancher/k3s/k3s.yaml off the group's first host and rewrites k3s's
// 127.0.0.1 to that host's address, so what comes back works from anywhere
// that can reach the lab -- a runner included.
//
// Not routed through call(): the body is a YAML document, not JSON, and
// parsing it as JSON would turn a perfectly good kubeconfig into { raw }.
async function kubeconfig(id) {
  const token = process.env.LABRES_TOKEN || ''
  if (!token) throw new Error('LABRES_TOKEN is empty')
  const res = await fetch(`${API}/reservations/${id}/kubeconfig`, {
    headers: { Authorization: `Bearer ${token}` },
  })
  const text = await res.text()
  if (res.status === 200) return text
  // The service answers errors as JSON even here, and its `code` is the part
  // worth reading: `invalid` is the reservation not having asked for k3s, or
  // not being active yet, which is a different problem from a 404.
  let code = ''
  try { code = JSON.parse(text).code || '' } catch { /* not JSON, show raw */ }
  throw new Error(
    `kubeconfig for ${id}: HTTP ${res.status}${code ? ` ${code}` : ''} ` +
    `${text.slice(0, 300)}`)
}

async function release(id) {
  const { status } = await call('DELETE', `/reservations/${id}`)
  // 404 means it is already gone -- expired, or released by something else.
  // That is the state we wanted, not a failure.
  return status === 202 || status === 200 || status === 404
}

const setOutput = (name, value) =>
  fs.appendFileSync(process.env.GITHUB_OUTPUT, `${name}=${value}\n`)

const saveState = (name, value) =>
  fs.appendFileSync(process.env.GITHUB_STATE, `${name}=${value}\n`)

module.exports = {
  BASE, input, warn, fail, log, call, existing, existingAll, reserve,
  waitReady, kubeconfig, release, setOutput, saveState,
}
