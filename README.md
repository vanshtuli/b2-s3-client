# Backblaze B2 S3 API Client

A Python client library for Backblaze B2 storage that provides AWS S3-like functionality. This library allows you to interact with Backblaze B2 storage using familiar S3-style methods in your Python applications.

## Features

- S3-like API for Backblaze B2 storage
- File upload and download
- Bucket listing and management
- File listing and information retrieval
- Sync functionality between local and B2 storage
- Copy operations between B2 locations
- Comprehensive error handling and logging

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/b2-s3-client.git
cd b2-s3-client
```

2. Install dependencies:
```bash
pip install b2sdk
```

## Configuration

Set up your Backblaze B2 credentials using environment variables:

```bash
export B2_APPLICATION_KEY_ID='your_application_key_id'
export B2_APPLICATION_KEY='your_application_key'
```

Or provide them directly when initializing the client:

```python
client = BackblazeB2Client(
    application_key_id='your_application_key_id',
    application_key='your_application_key'
)
```

## Usage

### Basic Operations

```python
from backblaze_b2 import BackblazeB2Client

# Initialize client
client = BackblazeB2Client()

# List buckets
buckets = client.list_buckets()

# List files in a bucket
files = client.list_files_in_bucket('bucket-name')

# Upload a file
client.upload_file('local/path/file.txt', 'bucket-name', 'remote/path/file.txt')

# Download a file
client.download_file('bucket-name', 'remote/path/file.txt', 'local/path/file.txt')

# Sync directories
client.sync('local/directory', 'b2://bucket-name/prefix/', delete=True)
```

## Features in Detail

- **Authentication**: Secure authentication with Backblaze B2 API
- **Bucket Operations**: List, create, and manage buckets
- **File Operations**: Upload, download, list, and delete files
- **Sync Operations**: Synchronize local directories with B2 storage
- **Copy Operations**: Copy files between B2 locations
- **Logging**: Comprehensive logging for debugging and monitoring

## Error Handling

The client includes robust error handling for:
- Authentication failures
- Network issues
- Invalid operations
- File not found errors
- Permission issues

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Backblaze B2 for providing the storage service
- b2sdk for the Python SDK 