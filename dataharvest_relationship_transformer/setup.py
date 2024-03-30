from setuptools import setup, find_packages

# Package metadata
NAME = 'scalable_relationship_transformer'
DESCRIPTION = 'Scalable Relationship transformer is reponsible for establishing relationship in persisted data'
VERSION = '1.0.0'
REQUIRES_PYTHON = '>=3.11'
AUTHOR = 'Sumant Kulkarni'
EMAIL = 'sumanthkulkarni10@email.com'

# Required packages
REQUIRED = [
    'boto3',
    'pydantic',
    'sqlalchemy',
    'sqlalchemy.orm',
]

# Optional packages
EXTRAS = {
    # Any extra dependencies, e.g., 'dev': ['pytest']
}

# Long description from README file
with open('README.md', 'r', encoding='utf-8') as f:
    LONG_DESCRIPTION = f.read()

# Setup configuration
setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    packages=find_packages(),
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.11',
    ],
)
