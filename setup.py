import setuptools


REQUIRED_PACKAGES = []
PACKAGE_NAME = 'my_alice_pipeline'
PACKAGE_VERSION = '1.0.0'
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Alice in Wonderland pipeline',
    url="https://github.com/horns-g/DataPipelines",
    author="Gabriele Corni",
    author_email="gabriele_corni@iprel.it",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
