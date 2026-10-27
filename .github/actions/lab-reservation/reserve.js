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
  BASE, input, warn, fail, log, call, reserve, waitReady, release,
  setOutput, saveState,
}
