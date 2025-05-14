import os

from setuptools import setup, find_packages

from setuptools.command.install import install as _install


def _post_install():
    from subprocess import getstatusoutput
    _, out = getstatusoutput('activate-global-python-argcomplete --user')
    if out:
        print(out)

    if os.environ.get("SHELL") and os.environ.get("HOME"):
        path = f"{os.environ.get('HOME')}/.bash_completion"
        if os.path.isfile(path):
            _, out = getstatusoutput(f'source {path}')
            found = False
            if os.path.exists(os.environ.get("HOME")+"/.bashrc"):
                with open(os.environ.get("HOME")+"/.bashrc", "r") as bashrc:
                    for line in bashrc.readlines():
                        line = line.strip()
                        if not line.startswith("#") and f"source {path}" in line:
                            found = True
                            break
            if not found:
                with open(os.environ.get("HOME") + "/.bashrc", "a") as bashrc:
                    bashrc.writelines([f"\nsource {path}\n"])


class install(_install):
    def run(self):
        _install.run(self)
        self.execute(_post_install, (), msg="Running post install task")


def get_env_var(name, default=None):
    if not name:
        return False
    with open("simplyblock_core/env_var", "r", encoding="utf-8") as fh:
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
        files = [f for f in os.listdir(src_dir) if os.path.isfile(f"{src_dir}/{f}") and f != ".DS_Store"]
        if not files:
            return []
        results.append((src_dir, [f"{src_dir}/{f}" for f in files]))
        dirs = [f for f in os.listdir(src_dir) if os.path.isdir(f"{src_dir}/{f}")]
        for dir in dirs:
            results.extend(gen_data_files(os.path.join(src_dir, dir)))
    return results


def get_long_description():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()


def get_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return fh.readlines()


COMMAND_NAME = get_env_var("SIMPLY_BLOCK_COMMAND_NAME", "sbcli")
VERSION = get_env_var("SIMPLY_BLOCK_VERSION", "1")

data_files = gen_data_files("simplyblock_core","simplyblock_web")
data_files.append(('', ["requirements.txt"]))
# data_files.append(('/etc/simplyblock', ["requirements.txt"]))


setup(
    name=COMMAND_NAME,
    version=VERSION,
    requires_python='>= 3.9',
    packages=find_packages(exclude=["e2e*"]),
    url='https://www.simplyblock.io/',
    author='Hamdy',
    author_email='hamdy@simplyblock.io',
    description='CLI for managing SimplyBlock cluster',
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    install_requires=get_requirements(),
    entry_points={
        'console_scripts': [
            f'{COMMAND_NAME}=simplyblock_cli.cli:main',
            'sbctl=simplyblock_cli.cli:main',
        ]
    },
    include_package_data=True,
    data_files=data_files,
    package_data={
        '': ["/etc/simplyblock/requirements.txt"],
        '/etc/simplyblock': ["requirements.txt"]
    },
    # cmdclass={'install': install},
)
