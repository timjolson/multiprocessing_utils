from setuptools import setup, find_packages


setup(
    name='mp_utils',
    version="0.5",
    description='python3 utils for multiprocessing',
    author = 'Tim Olson',
    author_email = 'tim.lsn@gmail.com',
    packages=find_packages(),
    tests_require=['pytest'],
)