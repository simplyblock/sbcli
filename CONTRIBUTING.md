# Contribution guidelines

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
