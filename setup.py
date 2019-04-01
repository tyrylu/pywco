from setuptools import setup

clfs = []
clfs.append("Development Status :: 4 - Beta")
clfs.append("Intended Audience :: Developers")
clfs.append("License :: OSI Approved :: MIT License")
clfs.append("Operating System :: Microsoft :: Windows :: Windows 7")
clfs.append("Operating System :: Microsoft :: Windows :: Windows 8")
clfs.append("Operating System :: Microsoft :: Windows :: Windows 8.1")
clfs.append("Operating System :: Microsoft :: Windows :: Windows 10")
clfs.append("Operating System :: MacOS :: MacOS X")
clfs.append("Operating System :: POSIX :: Linux")
clfs.append("Programming Language :: Python :: 3.5")

setup(name="pywco", version="0.1.0", author="Lukas Tyrychtr", author_email="lukastyrychtr@gmail.com", url="https://www.github.com/tyrylu/pywco", packages=["pywco"], long_description=open("README.md", "r").read(), description="Python based websockets communication helper.", license="MIT", classifiers=clfs, install_requires=["websockets", "blinker", "msgpack", "janus"])
