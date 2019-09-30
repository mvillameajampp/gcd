from setuptools import setup


setup(
    name="gcd",
    version="1.0",
    packages=["gcd"],
    package_data={"gcd": ["resources/*"]},
    scripts=["scripts/docdoc", "scripts/wacky"],
)
