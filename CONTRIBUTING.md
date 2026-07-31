# Contribution guidelines

## Installing several builds side by side

The distribution name is now static (`sbctl`), so installing a second build into an
environment replaces the first instead of sitting next to it.

Building with a custom `SIMPLY_BLOCK_COMMAND_NAME` used to produce a differently-named
distribution, which made `pip list` show both. That never gave you two versions: all builds
install the same `simplyblock_core`, `simplyblock_cli` and `simplyblock_web` packages, so the
second install silently overwrote the first's files — `env_var` included — and uninstalling
either one deleted the files out from under the other, leaving an entry in `pip list` with
nothing behind it.

Use one environment per version instead. `pipx` gives you the isolation and a distinct
command name in one step:

```bash
pipx install --suffix=-mybranch 'git+https://github.com/simplyblock/sbcli@mybranch'
sbctl-mybranch cluster list
```

## Error Handling Guidelines
All contributions to this repository must follow exception-based error handling practices. This applies to:

- **New code**: All newly introduced functions, methods, and modules
- **Modified code**: Any existing code that is touched or refactored as part of your changeset

When modifying existing code that uses other error handling patterns:

- Convert the touched code sections to use exceptions
- Update calling code within the same changeset if necessary
- Ensure backward compatibility is maintained where required

Pull requests that introduce or modify code without following these guidelines will require updates before merge.

### Requirements

When writing or modifying code, follow these guidelines:

#### Do
1. Use raise exceptions in error conditions
2. Throw specific, meaningful exceptions that clearly describe the error condition
    - use generic exceptions like `TypeError` and `ValueError` for generic failures
    - introduce specific exception types for specific error categories, like `APIError` or `StorageNodeError`
3. Handle exceptions appropriately at the right level of abstraction
4. Document expected exceptions in function/method docstrings

#### Don't
1. Use boolean flags, specific error values, or silent failures
2. Catch and ignore exceptions without proper logging or handling
3. Handle too general errors, e.g. `catch Exception`

### Examples

**✅ Good:**
```python
def divide(a, b):
    """Divide two numbers.

    Raises:
        ValueError: If b is zero
    """
    if b == 0:
        raise ValueError("Cannot divide by zero")
    return a / b
```

**❌ Avoid:**
```python
def divide(a, b):
    if b == 0:
        return None  # Silent failure
    return a / b
```
