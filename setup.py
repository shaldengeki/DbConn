try:
  from setuptools import setup
except ImportError:
  from distutils.core import setup

config = {
  'description': 'DbConn', 
  'author': 'Shal Dengeki', 
  'url': 'https://github.com/shaldengeki/DbConn', 
  'download_url': 'https://github.com/shaldengeki/DbConn', 
  'author_email': 'shaldengeki@gmail.com', 
  'version': '0.1', 
  'install_requires': ['nose'], 
  'packages': ['DbConn'], 
  'scripts': [],
  'name': 'DbConn'
}

setup(**config)