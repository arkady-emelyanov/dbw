from setuptools import setup

setup(
    name='dltx',
    version='0.1',
    packages=["dltx"],
    license='MIT',
    description='An example python package',
    long_description=open('README.md').read(),
    install_requires=[
        'databricks-cli',
        'click',
        'progress',
        'python-dotenv',
        'requests',
    ],
    url='https://github.com/johndoe/package',
    author='John Doe',
    author_email='nobody@example.com'
)
