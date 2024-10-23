from setuptools import setup, find_packages #type: ignore

setup(
    name='alphahelix_database_tools',
    version='1.4.8',
    author='Jeff Chen',
    author_email='jeffchen@alphahelix.com.tw',
    description='For Internal Use Only.',
    url="https://github.com/yahoo168/alphahelix_database_tools",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        'alphahelix_database_tools': ['cloud_database/config/data_route.xlsx', 'cloud_database/config/access.ini'],
    },
    install_requires=[
        "annotated-types==0.7.0",
        "anyio==4.4.0",
        "appnope==0.1.4",
        "asttokens==2.4.1",
        "cachetools==5.3.3",
        "certifi==2024.6.2",
        "charset-normalizer==3.3.2",
        "click==8.1.7",
        "comm==0.2.2",
        "debugpy==1.8.1",
        "decorator==5.1.1",
        "distro==1.9.0",
        "executing==2.0.1",
        "easyocr==1.7.2",
        "google-api-core==2.19.0",
        "google-api-python-client==2.134.0",
        "google-auth==2.30.0",
        "google-auth-httplib2==0.2.0",
        "google-auth-oauthlib==1.2.0",
        "google-cloud-core==2.4.1",
        "google-cloud-storage==2.17.0",
        "google-crc32c==1.5.0",
        "google-resumable-media==2.7.1",
        "googleapis-common-protos==1.63.1",
        "h11==0.14.0",
        "httpcore==1.0.5",
        "httplib2==0.22.0",
        "httpx==0.27.0",
        "idna==3.7",
        "ipykernel==6.29.4",
        "ipython==8.25.0",
        "jedi==0.19.1",
        "joblib==1.4.2",
        "jupyter_client==8.6.2",
        "jupyter_core==5.7.2",
        "matplotlib-inline==0.1.7",
        "matplotlib==3.9.2",
        "nest-asyncio==1.6.0",
        "nltk==3.8.1",
        "numpy==1.23.0",
        "oauthlib==3.2.2",
        "openai==1.35.3",
        "opencv-python==4.10.0.84",
        "packaging==24.1",
        "pandas==2.2.2",
        "parso==0.8.4",
        "pexpect==4.9.0",
        "platformdirs==4.2.2",
        "pillow==10.4.0",
        "prompt_toolkit==3.0.47",
        "proto-plus==1.24.0",
        "protobuf==4.25.3",
        "psutil==6.0.0",
        "ptyprocess==0.7.0",
        "pure-eval==0.2.2",
        "pyasn1==0.6.0",
        "pyasn1_modules==0.4.0",
        "pydantic==2.7.4",
        "pydantic_core==2.18.4",
        "pymongo==4.7.3",
        "python-dotenv==1.0.1",
        "Pygments==2.18.0",
        "PyMuPDF==1.24.5",
        "PyMuPDFb==1.24.3",
        "pyparsing==3.1.2",
        "python-dateutil==2.9.0.post0",
        "pytz==2024.1",
        "pyzmq==26.0.3",
        "regex==2024.5.15",
        "requests==2.32.3",
        "requests-oauthlib==2.0.0",
        "rsa==4.9",
        "setuptools==70.1.0",
        "six==1.16.0",
        "sniffio==1.3.1",
        "stack-data==0.6.3",
        "tiktoken==0.7.0",
        "tornado==6.4.1",
        "tqdm==4.66.4",
        "traitlets==5.14.3",
        "typing_extensions==4.12.2",
        "tzdata==2024.1",
        "uritemplate==4.1.1",
        "urllib3==2.2.2",
        "wcwidth==0.2.13"
        ],

    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)