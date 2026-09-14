# repo map: spdk

path: E:/spdk
head: ae5f4c025 (master)
ref: (working tree)
readable at: E:/spdk
generated: 2026-09-14T08:18:45Z
languages: c, cpp

## how to use this

GREP the two TSV files, do not read them. They are large by design;
reading one costs more context than it saves.

```bash
# where does a log message come from?  (the usual RCA entry point)
grep -i 'Forcing application shutdown' .agents/repo-maps/spdk.messages.tsv

# where is a function defined?
grep -P '^lvs_update_on_failover_cpl\t' .agents/repo-maps/spdk.symbols.tsv
```

Placeholders are normalised, so a message logged as
`f"port fence held {elapsed:.3f}s"` is indexed as
`port fence held {}s`. Grep for the stable words, not the values.

| index | entries | file |
|---|---|---|
| messages | 9571 | `spdk.messages.tsv` |
| symbols  | 11848 | `spdk.symbols.tsv` |

## layout

```
  lib/                                  335 files
  test/                                 198 files
  module/                               158 files
  include/                              125 files
  examples/                              24 files
  app/                                   14 files
```

## file types

```
  .c          582
  .h          269
  .cpp          2
  .cc           1
```

