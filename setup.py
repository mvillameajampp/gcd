import os
from setuptools import setup

current_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(current_dir, "gcd", "VERSION"), "r") as vf:
    version = vf.read().strip()

with open(os.path.join(current_dir, "README.rst")) as readme_file:
    readme = readme_file.read()


def parse_requirements_txt(filename="requirements.txt"):
    with open(os.path.join(current_dir, filename)) as requirements_file:
        requirements = requirements_file.readlines()
        # remove whitespaces
        requirements = [line.strip().replace(" ", "") for line in requirements]
        # remove all the requirements that are comments
        requirements = [line for line in requirements if not line.startswith("#")]
        # remove empty lines
        requirements = list(filter(None, requirements))
        return requirements


setup(
    name="gcd",
    description="Utils functions for Python3",
    version=version,
    packages=["gcd"],
    extras_require={
        "dev": parse_requirements_txt("requirements-dev.txt"),
        "all": parse_requirements_txt("requirements.txt"),
    },
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
    data_files=[("", ["requirements.txt", "requirements-dev.txt", "README.rst"])],
)
