#!/usr/bin/env python
# coding=utf-8
from simplyblock_cli.cli import CLIWrapper


COMMANDS = [
    "storage-node",
    "cluster",
    "lvol",
    "mgmt",
    "pool",
    "snapshot",
    "caching-node",
]


def get_parser_help_content(parser):
    formatter = parser._get_formatter()
    formatter.add_usage(None, parser._actions, parser._mutually_exclusive_groups)
    for action_group in parser._action_groups:
        formatter.start_section(action_group.title)
        formatter.add_text(action_group.description)
        formatter.add_arguments(action_group._group_actions)
        formatter.end_section()

    formatter.add_text(parser.epilog)
    return formatter.format_help()


def get_md_section(parser, title_level):
    return f"""
{title_level*"#"} {parser.description.capitalize()}
{parser.usage or ""}

```bash
{get_parser_help_content(parser)}
```

    """


cli = CLIWrapper()
parser = cli.parser
readme_content = get_md_section(parser, 1)

for cmd in COMMANDS:
    subparser = parser._subparsers._group_actions[0].choices[cmd]
    readme_content += get_md_section(subparser, 2)
    for i in subparser._subparsers._group_actions[0].choices:
        sub2 = subparser._subparsers._group_actions[0].choices[i]
        readme_content += get_md_section(sub2, 3)


print(readme_content)

with open("../simplyblock_cli/README.md", "w") as f:
    f.write(readme_content)

