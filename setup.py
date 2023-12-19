import os

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


def gen_data_files(*dirs):
    results = []
    for src_dir in dirs:
        files = [f for f in os.listdir(src_dir) if os.path.isfile(f"{src_dir}/{f}")]
        results.append((src_dir, [f"{src_dir}/{f}" for f in files]))
    return results


setup(
    name='sbcli',
    version='4.2.5',
    packages=find_packages(),
    url='https://github.com/',
    author='Hamdy Khader',
    author_email='hamdy.khader@gmail.com',
    description='CLI for managing SimplyBlock cluster',
    long_description=long_description,
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
            'sbcli=management.cli:main',
        ]
    },
    include_package_data=True,
    data_files=gen_data_files("management/scripts", "management/services", "management/spdk_installer"),
)
