import os
import setuptools

cur_directory_path = os.path.abspath(os.path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

print(os.path.join(cur_directory_path))

setuptools.setup(
    name="RefinitivAPIClient-leourb",
    version="2.0.1",
    author="Leonardo Urbano",
    author_email="leonardo.urbano@.com",
    packages=setuptools.find_packages(),
    package_data={'RefinitivAPIClient': ['json_requests/*.json']},
    include_package_data=True,
    description="A comprehensive Python Package for Refinitiv",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)