# repo map: ultra

path: E:/ultra
head: 94fcb14b (main)
ref: (working tree)
generated: 2026-09-14T08:12:16Z
languages: c, cpp, python

## how to use this

GREP the two TSV files, do not read them. They are large by design;
reading one costs more context than it saves.

```bash
# where does a log message come from?  (the usual RCA entry point)
grep -i 'Forcing application shutdown' .agents/repo-maps/ultra.messages.tsv

# where is a function defined?
grep -P '^lvs_update_on_failover_cpl\t' .agents/repo-maps/ultra.symbols.tsv
```

Placeholders are normalised, so a message logged as
`f"port fence held {elapsed:.3f}s"` is indexed as
`port fence held {}s`. Grep for the stable words, not the values.

| index | entries | file |
|---|---|---|
| messages | 4357 | `ultra.messages.tsv` |
| symbols  | 2963 | `ultra.symbols.tsv` |

## layout

```
  DISTR_v2/                             153 files
  Legacy/                                88 files
  api/                                   62 files
  testing/                               26 files
  WebApp/                                18 files
  scripts/                               11 files
  3rdparty/                               8 files
  ./                                      7 files
  PASS-TM/                                3 files
  Utilities/                              3 files
```

## file types

```
  .py         183
  .c           73
  .h           63
  .cpp         39
  .hpp         21
```

