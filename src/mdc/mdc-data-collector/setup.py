from setuptools import setup, find_packages

__version__ = '0.9.0'

LONG_DESCRIPTION = open("README.md", "r", encoding="utf-8").read()

setup(
    name="data_collector",
    version=__version__,
    author="Duk Kyu Lim",
    author_email="hong18s@gmail.com",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    description='',
    url="",
    license="MIT",
    keywords=["restful", "modbus", "api"],
    install_requires=[
        # 'flask',
        'flask_restx',
        'pymodbus',
        'PyYaml',
        'apscheduler',
    ],
    packages=find_packages(
        exclude=['dummy-modbus-server', 'dummy-modubs-server.*',
                 'tests', 'tests.*']),
    package_data={},
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Operating System :: MacOS",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
    ],
    entry_points={
        'console_scripts': [
            'restful-modbus-api=data_collector.__main__:main',
        ],
    },
    zip_safe=False,
)
