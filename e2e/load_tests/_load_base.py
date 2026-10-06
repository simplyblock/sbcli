"""Shared mechanics for load tests: sweep a parameter, measure, plot.

A load test is a different kind of thing from an e2e or a stress test, and
the difference is the output rather than the duration:

    e2e      pass / fail
    stress   pass / fail, but long and destructive
    load     A NUMBER, per step, across a swept parameter -- written to CSV,
             resumable across runs, and plotted at the end

"Did it pass" is close to meaningless for the third. The deliverable is a
curve: how does the thing you care about grow as the parameter grows, and
where does it stop being acceptable. ``lvol_outage_load.py`` established
this shape in the repo (lvol_count -> shutdown/restart seconds); this module
is that shape, factored out so the next one does not re-implement it.

Two deliberate differences from the original:

* **matplotlib is optional here.** ``lvol_outage_load`` imports it at module
  top, so on a runner without it the import of the WHOLE e2e package fails
  -- not the load test, the package, because ``e2e/__init__.py`` imports it
  eagerly. A missing plotting library should cost you the plot, not the
  suite.

* **A failed step is recorded, not fatal.** A sweep that dies at step three
  throws away the two measurements it already has. Those are the expensive
  part. A step that fails is written to the CSV with its error and the sweep
  continues, because "it worked at 10G and 50G and broke at 100G" is the
  finding, and it is lost if the run aborts.
"""
import csv
import os
import time
from pathlib import Path

from logger_config import setup_logger


class LoadSweepMixin:
    """CSV, resume and plotting for a swept measurement.

    Mix into a test that already knows how to bring up a cluster. The test
    supplies :meth:`measure_step`; this supplies everything around it.
    """

    #: Column names. First is the swept parameter; the rest are measurements.
    COLUMNS = ("step", "seconds")
    #: Written under e2e/logs/ unless the caller overrides output_file.
    DEFAULT_OUTPUT = "load.csv"
    #: Y-axis label for the plot.
    PLOT_TITLE = "Load sweep"
    PLOT_X = "parameter"
    PLOT_Y = "seconds"

    def init_sweep(self, **kwargs):
        self.output_dir = Path(kwargs.get("output_dir", "logs"))
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_file = self.output_dir / kwargs.get(
            "output_file", self.DEFAULT_OUTPUT)
        #: Resume rather than redo. A sweep is expensive and often gets
        #: killed; the steps already measured are still valid.
        self.continue_from_log = kwargs.get(
            "continue_from_log",
            os.environ.get("LOAD_CONTINUE", "1") not in ("0", "false", ""))
        self._load_log = setup_logger(__name__)

    # ── storage ──────────────────────────────────────────────────────────
    def record(self, row):
        """Append one measured step. Writes the header on first use."""
        new = not self.output_file.exists()
        with open(self.output_file, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(self.COLUMNS)
            w.writerow([row.get(c, "") for c in self.COLUMNS])
        self._load_log.info("[load] recorded %s -> %s", row, self.output_file)

    def previous(self):
        """Steps already measured, so a resumed sweep skips them."""
        if not (self.continue_from_log and self.output_file.exists()):
            return []
        out = []
        with open(self.output_file, newline="") as f:
            for r in csv.DictReader(f):
                out.append(r)
        return out

    def done_steps(self):
        key = self.COLUMNS[0]
        got = set()
        for r in self.previous():
            try:
                got.add(float(r[key]))
            except (KeyError, TypeError, ValueError):
                pass
        return got

    # ── the sweep ────────────────────────────────────────────────────────
    def sweep(self, steps):
        """Measure every step, recording each as it completes.

        A step that raises is recorded with its error and the sweep goes on.
        Aborting would discard the measurements already taken, which are the
        expensive part and are still true -- and "fine at 10G and 50G, broke
        at 100G" is itself the result.
        """
        already = self.done_steps()
        if already:
            self._load_log.info("[load] resuming; already measured: %s",
                                sorted(already))
        for s in steps:
            if float(s) in already:
                self._load_log.info("[load] step %s already in %s, skipping",
                                    s, self.output_file.name)
                continue
            self._load_log.info("=" * 62)
            self._load_log.info("[load] step %s", s)
            self._load_log.info("=" * 62)
            t0 = time.time()
            try:
                row = self.measure_step(s) or {}
                row.setdefault(self.COLUMNS[0], s)
                self.record(row)
            except Exception as exc:                  # noqa: BLE001
                self._load_log.error(
                    "[load] step %s FAILED after %.0fs: %s", s,
                    time.time() - t0, str(exc)[:300])
                self.record({self.COLUMNS[0]: s, "error": str(exc)[:200]})
        self.plot()
        self.summarise()

    def measure_step(self, step):
        """Measure one step; return a dict keyed by COLUMNS."""
        raise NotImplementedError

    # ── output ───────────────────────────────────────────────────────────
    def plot(self):
        rows = self.previous()
        if not rows:
            return
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except Exception as exc:                      # noqa: BLE001
            self._load_log.warning(
                "[load] no matplotlib (%s); the CSV at %s still has every "
                "measurement. A missing plotting library costs the plot, "
                "not the data.", str(exc)[:120], self.output_file)
            return

        x = []
        series = {c: [] for c in self.COLUMNS[1:]}
        for r in rows:
            try:
                xi = float(r[self.COLUMNS[0]])
            except (KeyError, TypeError, ValueError):
                continue
            x.append(xi)
            for c in self.COLUMNS[1:]:
                try:
                    series[c].append(float(r.get(c) or "nan"))
                except ValueError:
                    series[c].append(float("nan"))
        if not x:
            return

        plt.figure()
        for c, ys in series.items():
            if any(y == y for y in ys):               # any non-NaN
                plt.plot(x, ys, marker="o", label=c)
        plt.title(self.PLOT_TITLE)
        plt.xlabel(self.PLOT_X)
        plt.ylabel(self.PLOT_Y)
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        png = self.output_file.with_suffix(".png")
        plt.savefig(png)
        self._load_log.info("[load] plot written to %s", png)

    def summarise(self):
        """Print the curve, because the number is the point of the run."""
        rows = self.previous()
        if not rows:
            self._load_log.warning("[load] no measurements were recorded.")
            return
        self._load_log.info("")
        self._load_log.info("RESULT  %s", self.PLOT_TITLE)
        self._load_log.info("  %s", "  ".join(f"{c:>18}" for c in self.COLUMNS))
        for r in rows:
            self._load_log.info("  %s", "  ".join(
                f"{str(r.get(c, '')):>18}" for c in self.COLUMNS))
        errs = [r for r in rows if r.get("error")]
        if errs:
            self._load_log.warning(
                "[load] %d step(s) failed and are in the CSV with their "
                "error. The steps that DID measure are still valid, and "
                "where the curve stops is part of the answer.", len(errs))
