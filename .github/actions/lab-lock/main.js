const { input, warn, ensureSshpass, lock, saveRelease } = require('./lock.js')

const mode = input('mode')
const state = {
  hosts: input('hosts'),
  job: input('job'),
  label: input('label'),
  grace: input('grace-seconds'),
}

if (input('disabled') === 'true') {
  warn('Lab lock disabled', 'Hosts are NOT reserved; a concurrent run can wipe them.')
  process.exit(0)
}

if (!['acquire', 'verify', 'release'].includes(mode)) {
  console.log(`::error::unknown lab-lock mode '${mode}'`)
  process.exit(1)
}

if (mode === 'acquire') {
  saveRelease(state)
}

process.exit(ensureSshpass() ? lock(mode, state) : 1)
