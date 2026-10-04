const { spawnSync } = require('node:child_process')
const fs = require('node:fs')
const path = require('node:path')

const SCRIPT = path.resolve(__dirname, '../../../e2e/lab_lock.sh')

const input = (name) => process.env[`INPUT_${name.toUpperCase()}`] ?? ''

const warn = (title, message) => console.log(`::warning title=${title}::${message}`)

// sshpass is the one thing lab_lock.sh needs that a runner may not have, and
// the runner is not ours to provision. Idempotent, hence cheap to repeat.
function ensureSshpass() {
  const script = `
    set -euo pipefail
    command -v sshpass >/dev/null 2>&1 && exit 0
    if command -v dnf >/dev/null 2>&1; then sudo dnf install -y sshpass
    elif command -v yum >/dev/null 2>&1; then sudo yum install -y epel-release || true; sudo yum install -y sshpass
    elif command -v apt-get >/dev/null 2>&1; then sudo apt-get update -y && sudo apt-get install -y sshpass
    else echo "cannot install sshpass (unknown package manager)" >&2; exit 1
    fi`
  return spawnSync('bash', ['-c', script], { stdio: 'inherit' }).status === 0
}

function lock(mode, { hosts, job, label, grace }) {
  const args = [SCRIPT, mode, '--hosts', hosts]
  if (job) args.push('--job', job)
  if (label) args.push('--label', label)
  if (mode === 'acquire' && grace) args.push('--grace-seconds', grace)
  // status is null when the child died on a signal, which is a failure too.
  return spawnSync('bash', args, { stdio: 'inherit' }).status ?? 1
}

// What the post step needs to release, written before the hosts are taken:
// a cancellation lands mid-acquire often enough, and acquire's own rollback
// cannot run once it has been killed.
const saveRelease = (state) =>
  fs.appendFileSync(process.env.GITHUB_STATE, `release=${JSON.stringify(state)}\n`)

const pendingRelease = () =>
  process.env.STATE_release ? JSON.parse(process.env.STATE_release) : null

module.exports = { input, warn, ensureSshpass, lock, saveRelease, pendingRelease }
