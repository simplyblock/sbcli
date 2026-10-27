const {
  input, warn, fail, log, existing, reserve, waitReady, release,
  setOutput, saveState,
} = require('./reserve.js')

async function main() {
  if (input('disabled') === 'true') {
    warn('Lab reservation disabled',
         'The hosts are NOT reserved with labreservation.simplyblock.ai; ' +
         'someone can take them from under this run.')
    return 0
  }

  // A list: a lane's hosts are not always one cluster. The docker lab is
  // four -- the storage nodes, the management node, and a client each --
  // and a run that books only the storage nodes leaves the machines fio
  // runs on free for someone else to take mid-test.
  const clusters = input('cluster').split(/[\s,]+/).filter(Boolean)
  const kind = input('kind').trim()
  if (!!clusters.length === !!kind) {
    fail('give exactly one of `cluster` and `kind`; the API rejects both and neither')
    return 1
  }
  if (!process.env.LABRES_TOKEN) {
    fail('LABRES_TOKEN is not set. Add the API key from the reservations ' +
         'console as a repository secret and pass it to this action.')
    return 1
  }

  const created = []        // only these are released when the job ends
  const allHosts = []
  const names = []
  let expiresAt = ''

  // Each in turn. A failure part-way gives back what this job already took:
  // a lane holding three of four clusters it cannot use is worse than one
  // holding none, because the next run is then blocked by a booking nobody
  // is using.
  try {
    for (const one of (clusters.length ? clusters : [null])) {
      // Already ours? Then use it. Asking again answers 409 and fails a run
      // that was entitled to the machines all along -- an engineer holding
      // the lab for the afternoon, or an earlier job in the same chain.
      let res = await existing(one)
      let mine = false
      if (res) {
        log(`${res.cluster} is already reserved to this account as ${res.id} ` +
            `(expires ${res.expires_at}); using it rather than booking again`)
      } else {
        res = await reserve({
          cluster: one,
          kind,
          duration: input('duration') || '6h',
          clean: input('clean') === 'true',
          bootstrap: input('bootstrap') === 'true',
        })
        mine = true
        // Recorded before the wait, not after. The cluster is ours from the
        // 202, so a cancellation during the ready-poll would otherwise hold
        // it for its whole duration with nothing naming the id to release.
        //
        // Only a booking this job MADE is recorded, which is what the post
        // step gives back. Handing back one a person is sitting on, because
        // CI borrowed it for twenty minutes, is worse than a double booking.
        created.push(res.id)
        saveState('reservation', created.join(','))
        log(`reserved ${res.cluster} as ${res.id}, expires ${res.expires_at}`)
      }

      const r = res.ready === true && !mine
        ? res
        : await waitReady(res.id, Number(input('ready-timeout') || '900'))
      names.push(r.cluster)
      allHosts.push(...(r.hosts || []))
      if (!expiresAt || (r.expires_at && r.expires_at < expiresAt)) {
        expiresAt = r.expires_at || expiresAt
      }
      log(`ready: ${r.cluster} -> ${(r.hosts || []).join(' ') || '(no hosts listed)'}`)
    }
  } catch (err) {
    for (const id of created) {
      try { await release(id) } catch { /* reported below */ }
    }
    if (created.length) {
      log(`released ${created.length} booking(s) this job had already taken`)
    }
    throw err
  }

  const ready = { hosts: allHosts, cluster: names.join(' '),
                  expires_at: expiresAt, id: created.join(',') }
  const hosts = allHosts.join(' ')

  // The reservation covers the group as the lab inventory draws it, which is
  // not always the set a workflow touches. Checking it here turns "the run
  // wiped a machine nobody had reserved" into a line before anything is
  // destroyed.
  const expect = input('expect-hosts').split(/[\s,]+/).filter(Boolean)
  const missing = expect.filter((h) => !allHosts.includes(h))
  if (missing.length) {
    const msg = `reserved ${ready.cluster} does not cover ${missing.join(' ')}`
    if (input('require-hosts') === 'true') { fail(msg); return 1 }
    warn('Lab reservation', `${msg}. Those hosts are NOT reserved.`)
  }

  setOutput('id', ready.id)
  setOutput('cluster', ready.cluster)
  setOutput('hosts', hosts)
  setOutput('expires-at', ready.expires_at || '')
  setOutput('created', created.join(','))
  return 0
}

main().then((rc) => process.exit(rc)).catch((err) => {
  fail(err.message)
  process.exit(1)
})
