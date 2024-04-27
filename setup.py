from setuptools import setup
import os

VERSION = "0.4"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="datasette-enrichments",
    description="Tools for running enrichments against data stored in Datasette",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/simonw/datasette-enrichments",
    project_urls={
        "Issues": "https://github.com/simonw/datasette-enrichments/issues",
        "CI": "https://github.com/simonw/datasette-enrichments/actions",
        "Changelog": "https://github.com/simonw/datasette-enrichments/releases",
    },
    license="Apache License, Version 2.0",
    classifiers=[
        "Framework :: Datasette",
        "License :: OSI Approved :: Apache Software License",
    ],
    version=VERSION,
    packages=["datasette_enrichments"],
    entry_points={"datasette": ["enrichments = datasette_enrichments"]},
    install_requires=["datasette", "WTForms", "datasette-secrets>=0.2"],
    extras_require={
        "test": ["pytest", "pytest-asyncio", "black", "ruff", "packaging"],
        "docs": [
            "sphinx==7.2.6",
            "furo==2023.9.10",
            "sphinx-autobuild",
            "sphinx-copybutton",
            "myst-parser",
            "cogapp",
        ],
    },
    package_data={"datasette_enrichments": ["templates/*"]},
    python_requires=">=3.7",
)
