from distutils.core import setup

setup(
    name='elastichandler',
    version='0.1.0',
    description="Python logs to Elastic",
    author="Alexey Khit",
    install_requires=['pyyaml', 'requests'],
    packages=['elastichandler']
)
