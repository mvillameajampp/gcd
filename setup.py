import os
from setuptools import setup

current_dir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(current_dir, "gcd", "VERSION"), "r") as vf:
    version = vf.read().strip()


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
    version=version,
    packages=["gcd"],
    extras_require={
        "dev": parse_requirements_txt("requirements-dev.txt"),
        "all": parse_requirements_txt("requirements.txt"),
    },
    package_data={"gcd": ["VERSION"]},
    data_files=[("", ["requirements.txt", "requirements-dev.txt"])],
)
