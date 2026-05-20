import jinja2
import yaml
import sys
import re

from jsonschema import validators


def is_parameter(item):
    return item["name"].startswith("--") or item["name"].startswith("-")


def select_arguments(items):
    arguments = []
    for item in items:
        if not is_parameter(item):
            arguments.append(item)
    return arguments


def select_parameters(items):
    parameters = []
    for item in items:
        if is_parameter(item):
            parameters.append(item)
    return parameters


def no_newline(text):
    return re.sub("\n", "", text)


def argument_type(spec):
    if isinstance(spec, dict) and ((regex := spec.get('regex')) is not None):
        regex = escape_strings(regex)
        return f"regex_type(r'{regex}')"

    if spec == 'size':
        return "size_type()"

    if isinstance(spec, dict) and ((size := spec.get('size')) is not None):
        min = "utils.parse_size('{}')".format(size['min']) if 'min' in size else None
        max = "utils.parse_size('{}')".format(size['max']) if 'max' in size else None
        return f"size_type(min={min}, max={max})"

    if isinstance(spec, dict) and ((range := spec.get('range')) is not None):
        return f"range_type({range['min']}, {range['max']})"

    if spec == 'list':
        return "list_type()"

    return spec


def escape_python_string(text):
    return text.replace('%', '%%')


def choice_value(value):
    if isinstance(value, int):
        return str(value)
    return "'%s'" % value


def escape_strings(text):
    text = re.sub("'", "\\'", text)
    text = re.sub("\n", "", text)
    return text


def apply_deprecated_warning(item):
    deprecated = item.get("deprecated")
    if not deprecated:
        return ""

    since = deprecated.get("since")
    replaced_by = deprecated.get("replaced-by")

    if not since:
        raise ValueError("deprecated item must have a since")

    return f"**Deprecated since: {since}**{ '' if not replaced_by else f' Replaced by: {replaced_by}'}" + "\\n\\n"


def make_identifier(name):
    if name.startswith("--"):
        name = name[2:]
    return re.sub("-", "_", name.lower())


def bool_value(value):
    return value == "true"


def default_value(item):
    item_type = item["type"]
    if "default" not in item:
        return "None"
    value = item["default"]
    if item_type == "str":
        return "'%s'" % value
    elif item_type == "int":
        return value
    elif item_type == "bool":
        return value if isinstance(value, bool) else value.lower() == "true"
    elif item_type == "size" or (isinstance(item_type, dict) and 'size' in item_type):
        return f"'{value}'"
    elif isinstance(item_type, dict) and 'range' in item_type:
        return f"{value}"
    else:
        raise TypeError("unknown data type %s" % item_type)


def arg_value(item):
    if "action" in item:
        action = item["action"]
        if action == "store_true" or action == "store_false":
            return ""

    name = make_identifier(item["name"])
    return "=<%s>" % name


def get_description(item):
    if "description" in item:
        return no_newline(item["description"])
    elif "usage" in item:
        return no_newline(item["usage"])
    elif "help" in item:
        return no_newline(item["help"])
    else:
        return "<missing documentation>"


def nargs(item):
    value = item["nargs"]
    if not isinstance(value, int) and value not in ('?', '*', '+'):
        raise ValueError(f"Invalid nargs parameters: '{value}'")
    return value if isinstance(value, int) else f"'{value}'"


def validate_deprecated_parameters(items):
    for item in items:
        if "deprecated" in item:
            since = item["deprecated"].get("since")
            if not since:
                raise ValueError("deprecated parameter must have a since")

            if "replaced-by" in item["deprecated"]:
                found = False
                replaced_by = item["deprecated"]["replaced-by"]
                for replacement in items:
                    if replacement.get("name") == replaced_by:
                        found = True
                if not found:
                    raise ValueError(f"replaced-by parameter '{replaced_by}' not found in parameters")

            item.pop("default", None)


base_path = sys.argv[1]
with open("%s/cli-reference.yaml" % base_path) as stream:
    try:
        reference = yaml.safe_load(stream)
        # validate reference file against schema
        with open("%s/cli-reference-schema.yaml" % base_path) as schema:
            schema_content = yaml.safe_load(schema)
            validator_type = validators.validator_for(schema_content)
            validator = validator_type(schema_content)  # type: ignore
            errors = list(validator.iter_errors(reference))
            if errors:
                print("Generator failed on schema validation. Found the following errors:", file=sys.stderr)
                print('\n'.join(f" - {error.json_path}: {error.message}" for error in errors), file=sys.stderr)
                sys.exit(1)

        for command in reference["commands"]:
            for subcommand in command["subcommands"]:
                if "arguments" in subcommand:
                    arguments = select_arguments(subcommand["arguments"])
                    parameters = select_parameters(subcommand["arguments"])
                    validate_deprecated_parameters(parameters)
                    subcommand["arguments"] = arguments
                    subcommand["parameters"] = parameters

        templateLoader = jinja2.FileSystemLoader(searchpath="%s/scripts/" % base_path)
        environment = jinja2.Environment(loader=templateLoader)

        environment.filters["no_newline"] = no_newline
        environment.filters["argument_type"] = argument_type
        environment.filters["default_value"] = default_value
        environment.filters["get_description"] = get_description
        environment.filters["escape_strings"] = escape_strings
        environment.filters["make_identifier"] = make_identifier
        environment.filters["apply_deprecated_warning"] = apply_deprecated_warning
        environment.filters["bool_value"] = bool_value
        environment.filters["escape_python_string"] = escape_python_string
        environment.filters["nargs"] = nargs
        environment.filters["choice_value"] = choice_value

        template = environment.get_template("cli-wrapper.jinja2")
        output = template.render({"commands": reference["commands"]})
        with open("%s/cli.py" % base_path, "t+w") as target:
            target.write(output)

        print("Successfully generated cli.py")

    except yaml.YAMLError as exc:
        print(exc)
