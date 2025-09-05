import re


def single(xs):
    """Returns the single value in the passed collection

    If `xs` contains zero or multiple values, a ValueError error is raised.
    """

    it = iter(xs)

    try:
        x = next(it)
    except StopIteration:
        raise ValueError('No values present')

    try:
        next(it)
        raise ValueError('Multiple values present')
    except StopIteration:
        return x


def parse_thread_siblings_list(siblings: str) -> list[int]:
    if not siblings or not siblings.strip():
        return []

    cpus = set()
    token_re = re.compile(r"^\s*(?P<start>\d+)(?:\s*-\s*(?P<end>\d+)(?:\s*(?:[:\/])\s*(?P<step>\d+))?)?\s*$", re.X)

    for raw in siblings.split(","):
        part = raw.strip()
        if not part:
            continue
        m = token_re.match(part)
        if not m:
            raise ValueError(f"Invalid token in CPU list: {part!r}")

        start = int(m.group("start"))
        end = m.group("end")
        step = m.group("step")

        if end is None:
            cpus.add(start)
        else:
            end = int(end)
            if start > end:
                raise ValueError(f"Range start > end in token: {part!r}")
            step = int(step) if step is not None else 1
            if step <= 0:
                raise ValueError(f"Step must be positive in token: {part!r}")
            for v in range(start, end + 1, step):
                cpus.add(v)

    return sorted(cpus)
