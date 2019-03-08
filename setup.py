import setuptools


REQUIRED_PACKAGES = []
PACKAGE_NAME = 'my_alice_pipeline'
PACKAGE_VERSION = '1.0.0'
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description='Alice in Wonderland pipeline',
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
)
