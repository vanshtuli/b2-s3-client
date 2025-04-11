#!/usr/bin/env python3
"""
Backblaze B2 API Client
-----------------------
A simple client for interacting with Backblaze B2 storage.
"""

import os
import sys
import logging
from b2sdk.v2 import InMemoryAccountInfo, B2Api
import argparse
import concurrent.futures
from functools import partial

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BackblazeB2Client:
    """Client for interacting with Backblaze B2 Cloud Storage."""

    def __init__(self, application_key_id=None, application_key=None):
        """
        Initialize the Backblaze B2 client.

        Args:
            application_key_id (str): Backblaze B2 application key ID
            application_key (str): Backblaze B2 application key
        """
        self.application_key_id = application_key_id or os.environ.get(
            'B2_APPLICATION_KEY_ID')
        self.application_key = application_key or os.environ.get(
            'B2_APPLICATION_KEY')

        if not self.application_key_id or not self.application_key:
            raise ValueError(
                "Missing credentials. Please provide application_key_id and application_key "
                "or set B2_APPLICATION_KEY_ID and B2_APPLICATION_KEY environment variables."
            )

        # Set up the API client
        self.info = InMemoryAccountInfo()
        self.api = B2Api(self.info)
        self.authenticated = False

        self.authenticate()

    def authenticate(self):
        """Authenticate with Backblaze B2 API."""
        try:
            self.api.authorize_account(
                "production", self.application_key_id, self.application_key)
            self.authenticated = True
            logger.info("Successfully authenticated with Backblaze B2")
        except Exception as e:
            logger.error(f"Authentication failed: {str(e)}")
            raise

    def list_buckets(self):
        """
        List all buckets in the account.

        Returns:
            list: List of bucket objects
        """
        if not self.authenticated:
            self.authenticate()

        buckets = self.api.list_buckets()
        logger.info(f"Found {len(buckets)} buckets")
        return buckets

    def list_files_in_bucket(self, bucket_name, max_files=100, prefix=None):
        """
        List files in a specified bucket.

        Args:
            bucket_name (str): Name of the bucket
            max_files (int): Maximum number of files to list
            prefix (str): Optional prefix filter for filenames

        Returns:
            list: List of file information dictionaries
        """
        if not self.authenticated:
            self.authenticate()

        # Find the bucket
        bucket = self.api.get_bucket_by_name(bucket_name)
        if not bucket:
            raise ValueError(f"Bucket '{bucket_name}' not found")

        logger.info(f"Listing files in bucket '{bucket_name}'")

        # List files in the bucket
        file_versions = []
        count = 0

        # Using correct syntax for bucket.ls() as per documentation
        for file_version, folder_name in bucket.ls(latest_only=True):
            if prefix and not file_version.file_name.startswith(prefix):
                continue

            if count >= max_files:
                break

            file_versions.append({
                'file_name': file_version.file_name,
                'size': file_version.size,
                'content_type': file_version.content_type,
                'upload_timestamp': file_version.upload_timestamp,
                'id': file_version.id_
            })
            count += 1

        logger.info(
            f"Found {len(file_versions)} files in bucket '{bucket_name}'")
        return file_versions

    def get_file_info(self, bucket_name, file_name):
        """
        Get information about a specific file in a bucket.

        Args:
            bucket_name (str): Name of the bucket
            file_name (str): Name of the file

        Returns:
            dict: Information about the file
        """
        if not self.authenticated:
            self.authenticate()

        bucket = self.api.get_bucket_by_name(bucket_name)
        if not bucket:
            raise ValueError(f"Bucket '{bucket_name}' not found")

        # Get file info
        logger.info(
            f"Getting info for file '{file_name}' from bucket '{bucket_name}'")
        file_info = self.api.get_file_info_by_name(bucket_name, file_name)

        return {
            'file_name': file_info.file_name,
            'file_id': file_info.id_,
            'size': file_info.size,
            'content_type': file_info.content_type,
            'upload_timestamp': file_info.upload_timestamp
        }

    def download_file_by_id(self, file_id, local_path=None):
        """
        Download a file by its ID.

        Args:
            file_id (str): ID of the file to download
            local_path (str): Local path to save the file

        Returns:
            str: Local path where the file was saved
        """
        if not self.authenticated:
            self.authenticate()

        logger.info(f"Downloading file with ID '{file_id}'")

        # Download the file
        downloaded_file = self.api.download_file_by_id(file_id)

        # Set default local path if not specified
        if local_path is None:
            local_path = os.path.basename(downloaded_file.file_name)

        # Save to local path
        downloaded_file.save_to(local_path)

        logger.info(f"File downloaded to {local_path}")
        return local_path

    def download_file(self, bucket_name, file_name, local_path=None):
        """
        Download a file from a bucket.

        Args:
            bucket_name (str): Name of the bucket
            file_name (str): Name of the file to download
            local_path (str): Local path to save the file (defaults to file_name in current directory)

        Returns:
            str: Local path where the file was saved
        """
        if not self.authenticated:
            self.authenticate()

        # Get the bucket directly using the API
        bucket = self.api.get_bucket_by_name(bucket_name)
        if not bucket:
            raise ValueError(f"Bucket '{bucket_name}' not found")

        # Set default local path if not specified
        if local_path is None:
            local_path = os.path.basename(file_name)

        logger.info(
            f"Downloading '{file_name}' from bucket '{bucket_name}' to '{local_path}'")

        # Download the file
        downloaded_file = bucket.download_file_by_name(file_name)
        downloaded_file.save_to(local_path)

        logger.info(f"File downloaded to {local_path}")
        return local_path

    def upload_file(self, local_path, bucket_name, remote_path=None):
        """
        Upload a file to a bucket.

        Args:
            local_path (str): Path to the local file
            bucket_name (str): Name of the bucket
            remote_path (str): Remote path/name for the file (defaults to basename of local_path)

        Returns:
            dict: Information about the uploaded file
        """
        if not self.authenticated:
            self.authenticate()

        # Get the bucket
        bucket = self.api.get_bucket_by_name(bucket_name)
        if not bucket:
            raise ValueError(f"Bucket '{bucket_name}' not found")

        # Set default remote path if not specified
        if remote_path is None:
            remote_path = os.path.basename(local_path)

        logger.info(
            f"Uploading '{local_path}' to bucket '{bucket_name}' as '{remote_path}'")

        # Upload the file
        file_info = bucket.upload_local_file(
            local_file=local_path,
            file_name=remote_path
        )

        logger.info(f"File uploaded successfully with ID: {file_info.id_}")

        return {
            'file_name': file_info.file_name,
            'file_id': file_info.id_,
            'size': file_info.size,
            'content_type': file_info.content_type,
            'upload_timestamp': file_info.upload_timestamp
        }

    def cp(self, source, destination):
        """
        Copy a file to or from Backblaze B2 (similar to AWS cp command).

        The source and destination can be:
        - Local file path
        - B2 URI in the format "b2://bucket-name/path/to/file"

        Args:
            source (str): Source location (local path or B2 URI)
            destination (str): Destination location (local path or B2 URI)
        """
        if not self.authenticated:
            self.authenticate()

        # Helper function to parse B2 URI
        def parse_b2_uri(uri):
            if not uri.startswith("b2://"):
                return None, None

            parts = uri[5:].split("/", 1)
            bucket_name = parts[0]
            file_path = parts[1] if len(parts) > 1 else ""
            return bucket_name, file_path

        # Check if source is a B2 URI
        source_bucket, source_path = parse_b2_uri(source)

        # Check if destination is a B2 URI
        dest_bucket, dest_path = parse_b2_uri(destination)

        # Case 1: Download from B2 to local
        if source_bucket and not dest_bucket:
            result = self.download_file(
                source_bucket, source_path, destination)
            print(f"File downloaded successfully to {result}")
            return

        # Case 2: Upload from local to B2
        elif not source_bucket and dest_bucket:
            result = self.upload_file(source, dest_bucket, dest_path)
            print(f"File uploaded successfully: {result['file_name']}")
            print(f"  Size: {result['size'] / (1024 * 1024):.2f} MB")
            print(f"  ID: {result['file_id']}")
            return

        # Case 3: Copy from one B2 location to another
        elif source_bucket and dest_bucket:
            # Get the source bucket
            source_b = self.api.get_bucket_by_name(source_bucket)
            if not source_b:
                raise ValueError(f"Source bucket '{source_bucket}' not found")

            # Get the destination bucket
            dest_b = self.api.get_bucket_by_name(dest_bucket)
            if not dest_b:
                raise ValueError(
                    f"Destination bucket '{dest_bucket}' not found")

            logger.info(f"Copying from '{source}' to '{destination}'")

            # Copy the file within B2
            file_info = dest_b.copy(
                source_bucket_id=source_b.id_,
                source_file_name=source_path,
                file_name=dest_path
            )

            print(f"File copied successfully: {file_info.file_name}")
            print(f"  Size: {file_info.size / (1024 * 1024):.2f} MB")
            print(f"  ID: {file_info.id_}")
            return

        # Case 4: Local to local copy (not supported)
        else:
            raise ValueError(
                "Both source and destination are local paths. Use regular file operations instead.")

    def sync(self, source, destination, delete=False, exclude=None, include=None, max_workers=4):
        """
        Sync files between local filesystem and Backblaze B2 (similar to AWS sync command).

        The source and destination can be:
        - Local directory path
        - B2 URI in the format "b2://bucket-name/path/to/prefix/"

        Args:
            source (str): Source location (local path or B2 URI)
            destination (str): Destination location (local path or B2 URI)
            delete (bool): Whether to delete files in the destination that don't exist in the source
            exclude (list): Patterns to exclude from syncing
            include (list): Patterns to include in syncing (takes precedence over exclude)
            max_workers (int): Maximum number of parallel workers for file operations
        """
        if not self.authenticated:
            self.authenticate()

        # Set default values for exclude and include
        exclude = exclude or []
        include = include or []

        # Helper function to parse B2 URI
        def parse_b2_uri(uri):
            if not uri.startswith("b2://"):
                return None, None

            parts = uri[5:].split("/", 1)
            bucket_name = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
            return bucket_name, prefix

        # Check if source is a B2 URI
        source_bucket, source_prefix = parse_b2_uri(source)

        # Check if destination is a B2 URI
        dest_bucket, dest_prefix = parse_b2_uri(destination)

        # Helper function to check if a file should be included based on patterns
        def should_include(path):
            # If no include/exclude patterns, include everything
            if not include and not exclude:
                return True

            # Include patterns take precedence
            for pattern in include:
                if pattern in path:  # Simple contains check for now
                    return True

            # Check exclude patterns
            for pattern in exclude:
                if pattern in path:  # Simple contains check for now
                    return False

            # If no include patterns but has exclude patterns, include by default unless excluded
            return not include

        # Initialize sync statistics
        stats = {
            'files_uploaded': 0,
            'files_downloaded': 0,
            'files_deleted': 0,
            'bytes_transferred': 0,
            'errors': 0
        }

        # Helper function to process a single file upload
        def process_upload(local_path, remote_path, dest_b, stats):
            try:
                logger.info(f"Uploading {local_path} to {remote_path}")
                file_info = dest_b.upload_local_file(
                    local_file=local_path,
                    file_name=remote_path
                )
                stats['files_uploaded'] += 1
                stats['bytes_transferred'] += os.path.getsize(local_path)
            except Exception as e:
                logger.error(f"Error uploading {local_path}: {str(e)}")
                stats['errors'] += 1

        # Helper function to process a single file download
        def process_download(remote_key, local_path, source_b, stats):
            try:
                logger.info(f"Downloading {remote_key} to {local_path}")
                # Get file info first to get the size
                file_info = source_b.get_file_info_by_name(remote_key)
                downloaded_file = source_b.download_file_by_name(remote_key)
                downloaded_file.save_to(local_path)
                stats['files_downloaded'] += 1
                stats['bytes_transferred'] += file_info.size
            except Exception as e:
                logger.error(f"Error downloading {remote_key}: {str(e)}")
                stats['errors'] += 1

        # Case 1: Sync from local to B2
        if not source_bucket and dest_bucket:
            # Get the destination bucket
            dest_b = self.api.get_bucket_by_name(dest_bucket)
            if not dest_b:
                raise ValueError(f"Destination bucket '{dest_bucket}' not found")

            # Get list of files at destination
            dest_files = {}
            for file_version, _ in dest_b.ls(folder_to_list=dest_prefix, recursive=True, latest_only=True):
                key = file_version.file_name
                if dest_prefix and key.startswith(dest_prefix):
                    key = key[len(dest_prefix):]
                    if key.startswith('/'):
                        key = key[1:]

                dest_files[key] = {
                    'id': file_version.id_,
                    'size': file_version.size,
                    'modified': file_version.upload_timestamp
                }

            # Process uploads in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for root, _, files in os.walk(source):
                    for file in files:
                        local_path = os.path.join(root, file)
                        rel_path = os.path.relpath(local_path, source)

                        if not should_include(rel_path):
                            continue

                        if os.path.sep == '\\':  # Windows
                            rel_path = rel_path.replace('\\', '/')

                        remote_path = f"{dest_prefix}/{rel_path}" if dest_prefix else rel_path
                        remote_path = remote_path.lstrip('/')

                        local_size = os.path.getsize(local_path)
                        should_upload = True
                        if rel_path in dest_files:
                            if dest_files[rel_path]['size'] == local_size:
                                should_upload = False

                        if should_upload:
                            futures.append(executor.submit(
                                process_upload, local_path, remote_path, dest_b, stats))

                # Wait for all uploads to complete
                concurrent.futures.wait(futures)

            # Process deletions
            if delete:
                for key, info in dest_files.items():
                    local_path = os.path.join(source, key.replace('/', os.path.sep))
                    if not os.path.exists(local_path) and should_include(key):
                        try:
                            logger.info(f"Deleting {key} from B2")
                            dest_b.delete_file_version(info['id'], key)
                            stats['files_deleted'] += 1
                        except Exception as e:
                            logger.error(f"Error deleting {key}: {str(e)}")
                            stats['errors'] += 1

        # Case 2: Sync from B2 to local
        elif source_bucket and not dest_bucket:
            # Get the source bucket
            source_b = self.api.get_bucket_by_name(source_bucket)
            if not source_b:
                raise ValueError(f"Source bucket '{source_bucket}' not found")

            # Create destination directory if it doesn't exist
            if not os.path.exists(destination):
                os.makedirs(destination)

            # Get list of files in the source bucket
            source_files = {}
            for file_version, _ in source_b.ls(folder_to_list=source_prefix, recursive=True, latest_only=True):
                key = file_version.file_name
                if not should_include(key):
                    continue

                if source_prefix and key.startswith(source_prefix):
                    rel_key = key[len(source_prefix):]
                    if rel_key.startswith('/'):
                        rel_key = rel_key[1:]
                else:
                    rel_key = key

                source_files[rel_key] = {
                    'remote_key': key,
                    'id': file_version.id_,
                    'size': file_version.size,
                    'modified': file_version.upload_timestamp
                }

            # Process downloads in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for rel_key, info in source_files.items():
                    local_path = os.path.join(destination, rel_key.replace('/', os.path.sep))
                    os.makedirs(os.path.dirname(local_path), exist_ok=True)

                    should_download = True
                    if os.path.exists(local_path):
                        local_size = os.path.getsize(local_path)
                        if local_size == info['size']:
                            should_download = False

                    if should_download:
                        futures.append(executor.submit(
                            process_download, info['remote_key'], local_path, source_b, stats))

                # Wait for all downloads to complete
                concurrent.futures.wait(futures)

            # Process deletions
            if delete:
                for root, _, files in os.walk(destination):
                    for file in files:
                        local_path = os.path.join(root, file)
                        rel_path = os.path.relpath(local_path, destination)
                        if os.path.sep == '\\':  # Windows
                            rel_path = rel_path.replace('\\', '/')
                        if rel_path not in source_files and should_include(rel_path):
                            try:
                                logger.info(f"Deleting local file {local_path}")
                                os.remove(local_path)
                                stats['files_deleted'] += 1
                            except Exception as e:
                                logger.error(f"Error deleting {local_path}: {str(e)}")
                                stats['errors'] += 1

        # Case 3: B2 to B2 sync
        elif source_bucket and dest_bucket:
            # Get the source and destination buckets
            source_b = self.api.get_bucket_by_name(source_bucket)
            dest_b = self.api.get_bucket_by_name(dest_bucket)
            if not source_b or not dest_b:
                raise ValueError("Source or destination bucket not found")

            # Get list of files in both buckets
            source_files = {}
            dest_files = {}
            
            # Process source files
            for file_version, _ in source_b.ls(folder_to_list=source_prefix, recursive=True, latest_only=True):
                key = file_version.file_name
                if not should_include(key):
                    continue

                if source_prefix and key.startswith(source_prefix):
                    rel_key = key[len(source_prefix):]
                    if rel_key.startswith('/'):
                        rel_key = rel_key[1:]
                else:
                    rel_key = key

                source_files[rel_key] = {
                    'remote_key': key,
                    'id': file_version.id_,
                    'size': file_version.size,
                    'modified': file_version.upload_timestamp
                }

            # Process destination files
            for file_version, _ in dest_b.ls(folder_to_list=dest_prefix, recursive=True, latest_only=True):
                key = file_version.file_name
                if dest_prefix and key.startswith(dest_prefix):
                    rel_key = key[len(dest_prefix):]
                    if rel_key.startswith('/'):
                        rel_key = rel_key[1:]
                else:
                    rel_key = key

                dest_files[rel_key] = {
                    'remote_key': key,
                    'id': file_version.id_,
                    'size': file_version.size,
                    'modified': file_version.upload_timestamp
                }

            # Process copies in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for rel_key, source_info in source_files.items():
                    dest_key = f"{dest_prefix}/{rel_key}" if dest_prefix else rel_key
                    dest_key = dest_key.lstrip('/')

                    should_copy = True
                    if rel_key in dest_files:
                        if dest_files[rel_key]['size'] == source_info['size']:
                            should_copy = False

                    if should_copy:
                        futures.append(executor.submit(
                            lambda: dest_b.copy(
                                source_bucket_id=source_b.id_,
                                source_file_name=source_info['remote_key'],
                                file_name=dest_key
                            )))
                        stats['files_uploaded'] += 1
                        stats['bytes_transferred'] += source_info['size']

                # Wait for all copies to complete
                concurrent.futures.wait(futures)

            # Process deletions
            if delete:
                for rel_key, info in dest_files.items():
                    if rel_key not in source_files and should_include(rel_key):
                        try:
                            logger.info(f"Deleting {info['remote_key']} from B2")
                            dest_b.delete_file_version(info['id'], info['remote_key'])
                            stats['files_deleted'] += 1
                        except Exception as e:
                            logger.error(f"Error deleting {info['remote_key']}: {str(e)}")
                            stats['errors'] += 1

        # Case 4: Local to local sync (not supported)
        else:
            raise ValueError("Both source and destination are local paths. Use regular file operations instead.")

        logger.info(f"Sync completed: {stats}")

        print("\nSync completed:")
        print(f"  Files uploaded: {stats['files_uploaded']}")
        print(f"  Files downloaded: {stats['files_downloaded']}")
        print(f"  Files deleted: {stats['files_deleted']}")
        print(f"  Bytes transferred: {stats['bytes_transferred'] / (1024 * 1024):.2f} MB")

        if stats['errors'] > 0:
            print(f"  Errors: {stats['errors']} (check logs for details)")

    def list(self, bucket_name=None, prefix=None, max_files=100):
        """
        List buckets or files in a bucket and print the results.

        Args:
            bucket_name (str): Name of the bucket (optional)
            prefix (str): Prefix filter for file names (optional)
            max_files (int): Maximum number of files to list
        """
        if not self.authenticated:
            self.authenticate()

        if bucket_name:
            # List files in the bucket
            logger.info(f"Listing files in bucket '{bucket_name}'")
            try:
                files = self.list_files_in_bucket(
                    bucket_name, max_files=max_files, prefix=prefix)
                if not files:
                    print("No files found.")
                else:
                    for file in files:
                        size_mb = file['size'] / (1024 * 1024)
                        print(f"- {file['file_name']} ({size_mb:.2f} MB)")
            except ValueError as e:
                print(f"Error: {str(e)}")
        else:
            # List buckets
            logger.info("Listing all buckets")
            buckets = self.list_buckets()
            print("\nAvailable buckets:")
            for bucket in buckets:
                print(f"- {bucket.name}")


if __name__ == "__main__":
    client = BackblazeB2Client(
        application_key_id='0056330f53105c10000000001',
        application_key='K005+8GT9BYpCDbBHsT47OQrewGy+sE'
    )

    client.sync("b2://iqfeed/futures/cmemini/ES", "es")
