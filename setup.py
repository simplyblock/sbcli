import os

from setuptools import setup


def get_env_var(name, default=None):
    if not name:
        return False
    with open("env_var", "r", encoding="utf-8") as fh:
        lines = fh.readlines()
    data = {}
    for line in lines:
        if not line or line.startswith("#"):
            continue
        try:
            k, v = line.split("=")
            data[k.strip()] = v.strip()
        except:
            pass
    return data.get(name, default)


def gen_data_files(*dirs):
    results = []
    for src_dir in dirs:
        files = [f for f in os.listdir(src_dir) if os.path.isfile(f"{src_dir}/{f}")]
        results.append((src_dir, [f"{src_dir}/{f}" for f in files]))
    return results


def get_long_description():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()


COMMAND_NAME = get_env_var("SIMPLY_BLOCK_COMMAND_NAME", "sbcli")
VERSION = get_env_var("SIMPLY_BLOCK_VERSION", "1")

data_files = gen_data_files(
        "simplyblock_core/controllers",
        "simplyblock_core/models",
        "simplyblock_core/scripts",
        "simplyblock_core/services",
        "simplyblock_web/blueprints",
        "simplyblock_web/static",
        "simplyblock_web/templates")

data_files.append(('', ['env_var']))

setup(
    name=COMMAND_NAME,
    version=VERSION,
    packages=[
        'simplyblock_core',
        'simplyblock_core.controllers',
        'simplyblock_core.models',
        'simplyblock_core.scripts',
        'simplyblock_core.services',
        'simplyblock_cli', 'simplyblock_web', ],
    url='https://www.simplyblock.io/',
    author='Hamdy',
    author_email='hamdy@simplyblock.io',
    description='CLI for managing SimplyBlock cluster',
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    install_requires=[
        "foundationdb",
        "requests",
        "typing",
        "prettytable",
        "docker",
        "psutil",
        "py-cpuinfo",
    ],
    entry_points={
        'console_scripts': [
            f'{COMMAND_NAME}=simplyblock_cli.cli:main',
        ]
    },
    include_package_data=True,
    data_files=data_files,
)
