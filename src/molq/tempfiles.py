"""Temporary file management for molq.

This module provides the TempFileManager class for tracking and cleaning up
temporary files created during job submission.
"""

import os
from pathlib import Path


class TempFileManager:
    """Manages temporary file tracking and cleanup.
    
    This class tracks temporary files associated with jobs (e.g., submission
    scripts) and provides cleanup functionality when jobs complete.
    """

    def __init__(self):
        """Initialize the temp file manager."""
        self._temp_files_by_job: dict[int, list[str]] = {}

    def track(self, job_id: int, filepath: Path | str) -> None:
        """Track a temporary file for a specific job.
        
        Args:
            job_id: Job ID to associate the file with
            filepath: Path to the temporary file
        """
        filepath_str = str(filepath)
        
        if job_id not in self._temp_files_by_job:
            self._temp_files_by_job[job_id] = []
        
        if filepath_str not in self._temp_files_by_job[job_id]:
            self._temp_files_by_job[job_id].append(filepath_str)

    def cleanup(self, job_id: int) -> None:
        """Clean up temporary files for a specific job.
        
        Args:
            job_id: Job ID whose files should be cleaned up
        """
        if job_id not in self._temp_files_by_job:
            return
        
        for filepath in self._temp_files_by_job[job_id]:
            try:
                if os.path.exists(filepath):
                    os.unlink(filepath)
                    print(f"Cleaned up temporary file: {filepath}")
            except Exception as e:
                print(f"Warning: Could not clean up {filepath}: {e}")
        
        # Remove job from tracking
        del self._temp_files_by_job[job_id]

    def cleanup_all(self) -> None:
        """Clean up all tracked temporary files."""
        job_ids = list(self._temp_files_by_job.keys())
        for job_id in job_ids:
            self.cleanup(job_id)

    def get_tracked_files(self, job_id: int) -> list[str]:
        """Get list of tracked files for a job.
        
        Args:
            job_id: Job ID to query
            
        Returns:
            List of file paths tracked for this job
        """
        return self._temp_files_by_job.get(job_id, []).copy()
