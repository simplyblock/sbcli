const {
  input, warn, fail, log, existing, reserve, waitReady, setOutput, saveState,
} = require('./reserve.js')

async function main() {
  if (input('disabled') === 'true') {
    warn('Lab reservation disabled',
         'The hosts are NOT reserved with labreservation.simplyblock.ai; ' +
         'someone can take them from under this run.')
    return 0
  }

  const cluster = input('cluster').trim()
  const kind = input('kind').trim()
  if (!!cluster === !!kind) {
    fail('give exactly one of `cluster` and `kind`; the API rejects both and neither')
    return 1
  }
  if (!process.env.LABRES_TOKEN) {
    fail('LABRES_TOKEN is not set. Add the API key from the reservations ' +
         'console as a repository secret and pass it to this action.')
    return 1
  }

  // Already ours? Then use it. Asking again answers 409 and fails a run that
  // was entitled to the machines all along -- an engineer holding the lab for
  // the afternoon, or an earlier job in the same chain that took it.
  let res = await existing(cluster)
  let mine = false
  if (res) {
    log(`${res.cluster} is already reserved to this account as ${res.id} ` +
        `(expires ${res.expires_at}); using it rather than booking again`)
  } else {
    res = await reserve({
      cluster,
      kind,
      duration: input('duration') || '6h',
      clean: input('clean') === 'true',
      bootstrap: input('bootstrap') === 'true',
    })
    mine = true
    // Written before the wait, not after. The reservation is ours from the
    // 202, so a cancellation during the ready-poll would otherwise leave it
    // held for its whole duration with nothing recording the id to release.
    //
    // Only a reservation this job CREATED is recorded, which is what the post
    // step releases. Handing back one a person is sitting on, because a job
    // borrowed it for twenty minutes, would be worse than a double booking.
    saveState('reservation', res.id)
    log(`reserved ${res.cluster} as ${res.id}, expires ${res.expires_at}`)
  }

  const ready = res.ready === true && !mine
    ? res
    : await waitReady(res.id, Number(input('ready-timeout') || '900'))
  const hosts = (ready.hosts || []).join(' ')
  log(`ready: ${ready.cluster} -> ${hosts}`)

  // The reservation covers the group as the lab inventory draws it, which is
  // not always the set a workflow touches. Checking it here turns "the run
  // wiped a machine nobody had reserved" into a line before anything is
  // destroyed.
  const expect = input('expect-hosts').split(/[\s,]+/).filter(Boolean)
  const missing = expect.filter((h) => !(ready.hosts || []).includes(h))
  if (missing.length) {
    const msg = `reserved ${ready.cluster} does not cover ${missing.join(' ')}`
    if (input('require-hosts') === 'true') { fail(msg); return 1 }
    warn('Lab reservation', `${msg}. Those hosts are NOT reserved.`)
  }

  setOutput('id', ready.id)
  setOutput('cluster', ready.cluster)
  setOutput('hosts', hosts)
  setOutput('expires-at', ready.expires_at || '')
  setOutput('created', String(mine))
  return 0
}

main().then((rc) => process.exit(rc)).catch((err) => {
  fail(err.message)
  process.exit(1)
})
