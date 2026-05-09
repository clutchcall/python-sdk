from setuptools import setup, find_packages

setup(
    name="clutchcall-sdk",
    version="1.0.0",
    description="ClutchCall Polyglot SDK structured seamlessly over ALPN QUIC and local FFI boundaries.",
    packages=find_packages(),
    install_requires=[
        "aioquic>=0.9.21",
        "PyJWT>=2.6.0",
        "cryptography>=39.0.0"
    ],
    python_requires=">=3.7",
    package_data={
        "clutchcall": ["*.so", "*.dll", "*.dylib"]
    },
    include_package_data=True
)
