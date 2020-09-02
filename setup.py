from setuptools import setup, find_packages

setup(name='mpikat',
      version='0.1',
      description='FBFUSE and APSUSE interfaces for the MeerKAT CAM system',
      url='https://github.com/ewanbarr/mpikat',
      author='Ewan Barr',
      author_email='ebarr@mpifr-bonn.mpg.de',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'katpoint',
          'katcp',
          'ipaddress',
          'katportalclient',
          'posix_ipc',
          'jinja2',
          'coloredlogs'
      ],
      dependency_links=[
          'git+https://github.com/ska-sa/katportalclient.git',
      ],
      zip_safe=False)
