import os
from setuptools import setup

current_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(current_dir, "gcd", "VERSION"), "r") as vf:
    version = vf.read().strip()

with open(os.path.join(current_dir, "README.rst")) as readme_file:
    readme = readme_file.read()


extra_require = {"store": ["psycopg2"]}

all_dependencies = set()
for dependencies in extra_require.values():
    all_dependencies.update(dependencies)

extra_require["all"] = list(all_dependencies)


setup(
    name="gcd",
    description="Utils functions for Python3",
    version=version,
    packages=["gcd"],
    extras_require=extra_require,
    long_description=readme,
    long_description_content_type="text/x-rst",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development",
        "License :: OSI Approved :: BSD License",
    ],
    license="BSD 3-Clause",
    zip_safe=False,
    package_data={"gcd": ["VERSION"]},
)
