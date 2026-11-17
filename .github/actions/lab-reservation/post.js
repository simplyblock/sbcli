// The end of the run does NOT hand the lab back.
//
// It used to. The reasoning was that a 6h hold on a lab nobody is using is
// waste, and that is true -- but it is not what was actually happening. A
// stress run that fails leaves the cluster in the state it failed in, often
// deliberately (`preserve_resources_on_failure` is on by default), and the
// run after it begins by wiping those hosts. Releasing the reservation at the
// end of such a run tells the reservations console the machines are free. They
// are not free; they are holding a half-dead cluster that nobody has looked at
// yet, and the console saying "free" is how somebody takes them mid-triage or
// starts a run on a lab whose last occupant is still being read.
//
// So the booking is kept, and the NEXT run clears it immediately before
// booking again -- see main.js. The lab reads as busy for exactly as long as
// it is holding something, which is the honest answer, and `LABRES_DURATION`
// is the backstop: an unused booking still expires on its own.
//
// What this step does instead is say what is held and how to give it back, so
// "the lab shows busy" is never a mystery.
const { warn, log, existingAll } = require('./reserve.js')

const ids = (process.env.STATE_reservation || '').split(',').filter(Boolean)

async function main() {
  if (!ids.length) return

  const base = 'https://labreservation.simplyblock.ai/api/v1/reservations'
  log(`holding ${ids.length} reservation(s) past the end of this run, on ` +
      `purpose: ${ids.join(', ')}`)
  log('The next run on this lab releases them before it books. To give the ' +
      'lab back early -- when it is genuinely idle and not mid-triage -- ' +
      `run: ${ids.map((id) => `curl -X DELETE -H "Authorization: Bearer ` +
      `$LABRES_TOKEN" ${base}/${id}`).join(' ; ')}`)

  // Best effort, and only to report. A reservation that has already gone --
  // expired, or released by a concurrent run -- is worth a line, because then
  // the lab IS free and the hosts are in an unknown state with nothing
  // marking them.
  if (!process.env.LABRES_TOKEN) return
  try {
    const live = await existingAll()          // one call, not one per id
    for (const id of ids) {
      const row = live.find((r) => r.id === id)
      if (row) {
        log(`${id}: ${row.cluster} held until ${row.expires_at || 'unknown'}`)
      } else {
        warn('Lab reservation',
             `${id} is already gone. The lab now reads as free while its ` +
             `hosts are in whatever state this run left them -- which is the ` +
             `situation holding the booking exists to prevent. Expired, or ` +
             `released by a concurrent run?`)
      }
    }
  } catch (err) {
    warn('Lab reservation', `could not read back the reservations: ${err.message}`)
  }
}

main().catch((err) => warn('Lab reservation', `post step failed: ${err.message}`))
