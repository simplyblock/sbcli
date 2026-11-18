// Reserve the lab, clearing our own leftover booking first.
//
// The release used to happen in the post step, at the end of the run. It does
// not any more, and the order is the point: a run that ends does NOT hand the
// lab back, because the lab is usually not in a state anyone should take it in
// -- a failed stress run leaves a cluster at 4 of 6 nodes with resources
// preserved for debugging, and a reservation released at that moment tells the
// console the machines are free when what is actually free is a wreck. Holding
// the booking is the honest signal.
//
// So the clearing happens here instead, immediately before a new booking, by
// the run that is about to wipe those hosts anyway. See post.js.
const fs = require('node:fs')
const path = require('node:path')
const os = require('node:os')
const {
  input, warn, fail, log, existing, existingAll, waitCleared, reserve,
  waitReady, kubeconfig, release, setOutput, saveState,
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
  // The reservation that had k3s stood up for it, if any. Only one of a lane's
  // clusters is ever the Kubernetes one -- the others are clients and a
  // management node -- so the first that reports `bootstrap` is the cluster
  // whose kubeconfig a caller means.
  let bootstrapped = null

  // Each in turn. A failure part-way gives back what this job already took:
  // a lane holding three of four clusters it cannot use is worse than one
  // holding none, because the next run is then blocked by a booking nobody
  // is using.
  try {
    for (const one of (clusters.length ? clusters : [null])) {
      // Already ours? Two answers, and which one applies is a policy choice.
      //
      // `reuse` is the old behaviour and still the right one for a booking a
      // person is sitting on -- an engineer holding the lab for the afternoon,
      // or an earlier job in the same chain. Asking again answers 409 and
      // fails a run that was entitled to the machines all along.
      //
      // `release` is the default now. The previous run's booking is debris by
      // the time anyone reads it: the run is over, the hosts are in whatever
      // state it left them, and this job is about to wipe them regardless.
      // Clearing it here rather than at the end of that run is what keeps the
      // console honest in between.
      let res = await existing(one)
      let mine = false
      if (res && input('on-existing') !== 'reuse') {
        // Loud, and with the holder in it. If this ever evicts a person, the
        // line that did it has to be findable without reading the action.
        for (const stale of await existingAll(one)) {
          log(`clearing our own existing booking ${stale.id} on ` +
              `${stale.cluster} (state=${stale.state} ` +
              `expires=${stale.expires_at || '-'}) before booking again -- ` +
              `this run is about to clean those hosts`)
          if (!await release(stale.id)) {
            warn('Lab reservation',
                 `could not release ${stale.id}; the booking below may answer ` +
                 `409. Release it by hand with: curl -X DELETE -H ` +
                 `"Authorization: Bearer $LABRES_TOKEN" ` +
                 `https://labreservation.simplyblock.ai/api/v1/reservations/${stale.id}`)
          }
        }
        // And then wait for the cluster to actually come free. The DELETE
        // above is accepted, not done: the teardown playbook runs in the
        // background and the service holds the cluster for the whole of it,
        // so booking straight afterwards books against ourselves. Both k3s
        // attempts on 2026-11-18 died here, reporting the lab as "held by
        // raunak@simplyblock.io until ..." with the expiry of the booking
        // this action had released a quarter of a second earlier.
        await waitCleared(one, Number(input('ready-timeout') || '900'))
        res = null
      }
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
      if (!bootstrapped && r.bootstrap === true) bootstrapped = r
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

  // The kubeconfig, for a lane whose cluster this service built.
  //
  // A path, not the document: a kubeconfig in an output is a kubeconfig in
  // the step log and in every later `env` dump, and it carries a working
  // client certificate for the cluster. The file lands in RUNNER_TEMP, which
  // the runner clears between jobs.
  //
  // A failure here does not fail the reservation. The hosts are booked and
  // the cluster is up; a lane that does not want the kubeconfig must not lose
  // its lab because fetching one did not work.
  let kubePath = ''
  if (bootstrapped) {
    try {
      const content = await kubeconfig(bootstrapped.id)
      const dir = process.env.RUNNER_TEMP || os.tmpdir()
      kubePath = path.join(dir, `labres-kubeconfig-${bootstrapped.cluster}.yaml`)
      fs.writeFileSync(kubePath, content, { mode: 0o600 })
      log(`kubeconfig for ${bootstrapped.cluster} written to ${kubePath} ` +
          `(server ${(content.match(/server:\s*(\S+)/) || [])[1] || '?'})`)
    } catch (err) {
      kubePath = ''
      warn('Lab reservation',
           `${bootstrapped.cluster} was bootstrapped but its kubeconfig could ` +
           `not be fetched: ${err.message}. The reservation stands; a lane ` +
           `that needs the kubeconfig will fail on the empty output.`)
    }
  }

  setOutput('kubeconfig', kubePath)
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
