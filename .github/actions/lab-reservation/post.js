const { warn, log, release } = require('./reserve.js')

const id = process.env.STATE_reservation

// Releasing must never be the reason a run is reported failed: by the time
// this runs the result is already decided. It is also not strictly required --
// reservations expire on their own -- but a 6h hold on a lab nobody is using
// is the thing this whole exercise was meant to stop, so the warning is loud.
async function main() {
  if (!id) return
  if (!process.env.LABRES_TOKEN) {
    warn('Lab reservation', `no token in the post step; reservation ${id} left to expire`)
    return
  }
  if (await release(id)) {
    log(`released ${id}`)
  } else {
    warn('Lab reservation',
         `could not release ${id}; it will expire on its own, or release it ` +
         `with: curl -X DELETE -H "Authorization: Bearer $LABRES_TOKEN" ` +
         `https://labreservation.simplyblock.ai/api/v1/reservations/${id}`)
  }
}

main().catch((err) => warn('Lab reservation', `release failed: ${err.message}`))
