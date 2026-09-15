const { warn, ensureSshpass, lock, pendingRelease } = require('./lock.js')

const state = pendingRelease()

// Release must never be the reason a run is reported as failed. A lock left
// behind blocks the lab until someone runs `break`, so the warning has to be
// loud, but the run's own result is already known by the time this runs.
if (state && (!ensureSshpass() || lock('release', state) !== 0)) {
  warn('Lab lock', "release failed; clear it with 'e2e/lab_lock.sh break --force'")
}
