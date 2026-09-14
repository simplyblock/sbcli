# repo map: operator

path: E:/simplyblock-operator
head: d7d3f316 (main)
ref: (working tree)
generated: 2026-09-14T08:12:22Z
languages: go, python

## how to use this

GREP the two TSV files, do not read them. They are large by design;
reading one costs more context than it saves.

```bash
# where does a log message come from?  (the usual RCA entry point)
grep -i 'Forcing application shutdown' .agents/repo-maps/operator.messages.tsv

# where is a function defined?
grep -P '^lvs_update_on_failover_cpl\t' .agents/repo-maps/operator.symbols.tsv
```

Placeholders are normalised, so a message logged as
`f"port fence held {elapsed:.3f}s"` is indexed as
`port fence held {}s`. Grep for the stable words, not the values.

| index | entries | file |
|---|---|---|
| messages | 11028 | `operator.messages.tsv` |
| symbols  | 6449 | `operator.symbols.tsv` |

## layout

```
  operator/                             435 files
  atlas-lib/                            233 files
  csi-driver/                            91 files
  test/                                  37 files
  .claude/                               14 files
  helm-charts/                            1 files
  shared/                                 1 files
```

## file types

```
  .go         795
  .py          17
```

