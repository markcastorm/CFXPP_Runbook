"""
CFXPP File Archiver
====================
Moves processed input files to batch-organized archive folders.
"""

import os
import shutil
import logging

import config

logger = logging.getLogger(__name__)


class CFXPPArchiver:
    """Archives processed input files by batch."""

    def __init__(self):
        self.archive_base = config.ARCHIVE_DIR

    def get_archive_path(self, batch_id):
        """
        Get archive directory path for a batch, creating it if needed.

        Args:
            batch_id: Batch identifier string (e.g., '20260320_20260325').

        Returns:
            str: Absolute path to the archive subdirectory.
        """
        archive_dir = os.path.join(self.archive_base, batch_id)
        os.makedirs(archive_dir, exist_ok=True)
        return archive_dir

    def archive_file(self, file_path, batch_id):
        """
        Move a single processed file to the archive.

        Args:
            file_path: Absolute path to the file to archive.
            batch_id: Batch identifier string.

        Returns:
            str: New path of the archived file, or None on failure.
        """
        if not os.path.exists(file_path):
            logger.warning(f'File not found for archiving: {file_path}')
            return None

        archive_dir = self.get_archive_path(batch_id)
        dest = os.path.join(archive_dir, os.path.basename(file_path))

        # Handle name collisions
        if os.path.exists(dest):
            base, ext = os.path.splitext(os.path.basename(file_path))
            counter = 1
            while os.path.exists(dest):
                dest = os.path.join(archive_dir, f'{base}_{counter}{ext}')
                counter += 1

        try:
            shutil.move(file_path, dest)
            logger.debug(f'Archived: {os.path.basename(file_path)} -> {batch_id}/')
            return dest
        except Exception as e:
            logger.error(f'Failed to archive {file_path}: {e}')
            return None

    def archive_batch(self, file_paths, batch_id):
        """
        Move all files from a batch to archive.

        Args:
            file_paths: List of file paths to archive.
            batch_id: Batch identifier string.

        Returns:
            tuple: (archived_count, failed_count)
        """
        archived = 0
        failed = 0

        for fp in file_paths:
            result = self.archive_file(fp, batch_id)
            if result:
                archived += 1
            else:
                failed += 1

        logger.info(f'Archived {archived} files to {batch_id}/ ({failed} failures)')
        return archived, failed

    def list_archived_batches(self):
        """Return list of existing batch IDs in archive."""
        if not os.path.exists(self.archive_base):
            return []
        return sorted([
            d for d in os.listdir(self.archive_base)
            if os.path.isdir(os.path.join(self.archive_base, d))
        ])
