from setuptools import setup

setup(name='pyramid_safile',
      version='0.1',
      description='Sqlalchmy and files for pyramid',
      url='https://github.com/rickmak/pyramid_safile',
      author='Rick Mak',
      author_email='rickmak@oursky.com',
      license='MIT',
      packages=['pyramid_safile'],
      install_requires=[
          'tinys3',
      ],
      zip_safe=False)